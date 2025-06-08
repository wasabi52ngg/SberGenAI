import os
import json
import uuid
import re
import sqlite3
import shutil
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from openai import OpenAI
import logging
import pandas as pd
import asyncio
import telegram
import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
    Application
)
from telegram.error import TimedOut

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Загрузка переменных из .env файла
load_dotenv()

# Настройка токенов
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
API_KEY = os.getenv('API_KEY')
PROXY_API_URL = os.getenv('PROXY_API_URL')

# URL-адреса сервисов
SERVICE_URLS = {
    "gibdd_auto": "http://localhost:5003/gibdd",
    "gibdd_fines": "http://localhost:5004/fines",
    "efrsb": "http://localhost:5001/efrsb",
    "kad_arbitr": "http://localhost:5002/kad_arbitr",
    "osago": "http://localhost:5006/osago",
    "nalog": "http://localhost:5007/nalog",
    "pledge": "http://localhost:5008/pledge",
    "probate": "http://localhost:5005/probate"
}

# Загрузка системного промпта
SYSTEM_PROMPT_PATH = "system_prompt.txt"
with open(SYSTEM_PROMPT_PATH, "r", encoding="utf-8") as f:
    SYSTEM_PROMPT = f.read() + "\nФорматируйте ответ в виде простого текста без использования Markdown, заголовков, списков или других элементов форматирования."

# Инициализация клиента OpenAI
try:
    client = OpenAI(
        api_key=API_KEY,
        base_url=PROXY_API_URL,
    )
    logger.info("Клиент OpenAI успешно инициализирован")
except Exception as e:
    logger.error(f"Ошибка при инициализации клиента OpenAI: {e}")
    print(f"Ошибка при инициализации клиента OpenAI: {e}")
    exit(1)

# Настройка базы данных SQLite и логов
DB_PATH = "debtors.db"
BACKUP_DIR = "backups"
LOG_FILE = "updates_log.txt"
EXAMPLE_FILE = "Пример.xlsx"
if not os.path.exists(BACKUP_DIR):
    os.makedirs(BACKUP_DIR)

# Инициализация планировщика
scheduler = AsyncIOScheduler()

# Поля таблицы для редактирования
EDITABLE_FIELDS = [
    "inn", "fio", "vin", "sts", "grz", "gibdd_auto", "gibdd_fines", "efrsb",
    "nsis", "reestr_zalogov", "notariat", "pb_nalog", "kad_arbitr"
]
FIELDS_PER_PAGE = 8  # Количество полей на одной странице кнопок
request_queue = asyncio.Queue()

async def worker(context: ContextTypes.DEFAULT_TYPE):
    """Фоновая задача для обработки очереди запросов."""
    while True:
        try:
            # Получаем задачу из очереди
            data, update, is_excel = await request_queue.get()
            logger.info(f"Воркер взял запрос для ИНН {data.get('inn', 'не указан')} от пользователя {update.effective_user.id}")
            if is_excel:
                await process_excel_row(data, update, context)
            else:
                await process_single_request(data, update, context)
            request_queue.task_done()
        except Exception as e:
            logger.error(f"Ошибка в воркере: {str(e)}", exc_info=True)
            await asyncio.sleep(2)


async def process_excel_row(data: dict, update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка одной строки Excel из очереди."""
    inn = data.get("inn", "")
    waiting_message = None
    try:
        waiting_message = await update.message.reply_text(f"Обработка ИНН {inn}. Пожалуйста, ожидайте.")

        collected_data = {
            "inn": data.get("inn", ""),
            "fio": data.get("fio", ""),
            "vin": data.get("vin", ""),
            "sts": data.get("sts", ""),
            "grz": data.get("grz", ""),
            "gibdd_auto": "",
            "gibdd_fines": "",
            "efrsb": "",
            "nsis": "",
            "reestr_zalogov": "",
            "notariat": "",
            "pb_nalog": "",
            "kad_arbitr": ""
        }

        async with aiohttp.ClientSession() as session:
            tasks = []
            if collected_data["vin"]:
                tasks.extend([
                    fetch_service_data("gibdd_auto", {"vin": collected_data["vin"]}),
                    fetch_service_data("osago", {"vin": collected_data["vin"]}),
                    fetch_service_data("pledge", {"vin": collected_data["vin"]})
                ])
            if collected_data["grz"] and collected_data["sts"]:
                grz_parts = collected_data["grz"].split()
                if len(grz_parts) == 2:
                    regnum, regreg = grz_parts
                    tasks.append(fetch_service_data("gibdd_fines", {
                        "regnum": regnum,
                        "regreg": regreg,
                        "stsnum": collected_data["sts"]
                    }))
                else:
                    collected_data["gibdd_fines"] = json.dumps(
                        {"status": "error", "message": "Неверный формат ГРЗ"}, ensure_ascii=False)
            if collected_data["inn"]:
                tasks.extend([
                    fetch_service_data("efrsb", {"inn": collected_data["inn"]}),
                    fetch_service_data("nalog", {"inn": collected_data["inn"]}),
                    fetch_service_data("kad_arbitr", {"inn": collected_data["inn"]})
                ])
            if collected_data["fio"]:
                parts = collected_data["fio"].split(';')
                if len(parts) == 2:
                    name, birth_date = parts
                    tasks.append(fetch_service_data("probate", {
                        "name": name.strip(),
                        "birth_date": birth_date.strip()
                    }))
                else:
                    tasks.append(fetch_service_data("probate", {
                        "name": collected_data["fio"].strip(),
                        "birth_date": ""
                    }))

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
            else:
                results = []

            task_idx = 0
            if collected_data["vin"]:
                collected_data["gibdd_auto"] = json.dumps(results[task_idx], ensure_ascii=False)
                collected_data["nsis"] = json.dumps(results[task_idx + 1], ensure_ascii=False)
                collected_data["reestr_zalogov"] = json.dumps(results[task_idx + 2], ensure_ascii=False)
                task_idx += 3
            if collected_data["grz"] and collected_data["sts"] and len(collected_data["grz"].split()) == 2:
                collected_data["gibdd_fines"] = json.dumps(results[task_idx], ensure_ascii=False)
                task_idx += 1
            if collected_data["inn"]:
                collected_data["efrsb"] = json.dumps(results[task_idx], ensure_ascii=False)
                collected_data["pb_nalog"] = json.dumps(results[task_idx + 1], ensure_ascii=False)
                collected_data["kad_arbitr"] = json.dumps(results[task_idx + 2], ensure_ascii=False)
                task_idx += 3
            if collected_data["fio"]:
                collected_data["notariat"] = json.dumps(results[task_idx], ensure_ascii=False)

        save_to_db(collected_data)
        logger.info(f"Обработана строка для ИНН {inn}")

        data_summary = (
            f"ИНН: {collected_data['inn'] or 'Не указано'}\n"
            f"ФИО: {collected_data['fio'] or 'Не указано'}\n"
            f"VIN: {collected_data['vin'] or 'Не указано'}\n"
            f"СТС: {collected_data['sts'] or 'Не указано'}\n"
            f"ГРЗ: {collected_data['grz'] or 'Не указано'}\n"
            f"ГИБДД (авто): {collected_data['gibdd_auto'] or 'Данные отсутствуют'}\n"
            f"ГИБДД (штрафы): {collected_data['gibdd_fines'] or 'Данные отсутствуют'}\n"
            f"ЕФРСБ: {collected_data['efrsb'] or 'Данные отсутствуют'}\n"
            f"НСИС: {collected_data['nsis'] or 'Данные отсутствуют'}\n"
            f"Реестр залогов: {collected_data['reestr_zalogov'] or 'Данные отсутствуют'}\n"
            f"Нотариат: {collected_data['notariat'] or 'Данные отсутствуют'}\n"
            f"ПБ Налог: {collected_data['pb_nalog'] or 'Данные отсутствуют'}\n"
            f"Кад.арбитр: {collected_data['kad_arbitr'] or 'Данные отсутствуют'}"
        )

        chat_completion = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Проанализируйте данные о должнике и составьте портрет:\n{data_summary}"}
            ]
        )
        response = chat_completion.choices[0].message.content
        logger.info(f"Ответ от DeepSeek для ИНН {inn} получен")

        await waiting_message.delete()
        for attempt in range(3):
            try:
                await update.message.reply_text(f"Портрет должника (ИНН {inn}):\n\n{response}")
                break
            except TimedOut:
                logger.warning(f"Тайм-аут при отправке портрета для ИНН {inn} (попытка {attempt + 1}/3)")
                await asyncio.sleep(2)
        else:
            logger.error(f"Не удалось отправить портрет для ИНН {inn} после 3 попыток")
            await update.message.reply_text("Ошибка связи с сервером. Пожалуйста, попробуйте снова.")

    except Exception as e:
        logger.error(f"Ошибка при обработке ИНН {inn}: {str(e)}")
        if waiting_message:
            await waiting_message.delete()
        await update.message.reply_text(f"Ошибка обработки ИНН {inn}: {str(e)}")

def init_db():
    """Инициализация базы данных."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS debtors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                inn TEXT UNIQUE,
                fio TEXT,
                vin TEXT,
                sts TEXT,
                grz TEXT,
                gibdd_auto TEXT,
                gibdd_fines TEXT,
                efrsb TEXT,
                nsis TEXT,
                reestr_zalogov TEXT,
                notariat TEXT,
                pb_nalog TEXT,
                kad_arbitr TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()


def backup_db():
    """Создание бэкапа базы данных."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(BACKUP_DIR, f"debtors_backup_{timestamp}.db")
        shutil.copyfile(DB_PATH, backup_path)
        logger.info(f"Бэкап базы данных создан: {backup_path}")
    except Exception as e:
        logger.error(f"Ошибка при создании бэкапа: {e}")


def log_updates(inn: str, changes: dict):
    """Логирование изменений в данных."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] Изменения для ИНН {inn}:\n")
        for field, (old_value, new_value) in changes.items():
            f.write(f"  Поле {field}: {old_value} -> {new_value}\n")
        f.write("\n")
    logger.info(f"Изменения для ИНН {inn} записаны в лог")


async def fetch_service_data(service: str, payload: dict, max_attempts: int = 3, check_interval: int = 10) -> dict:
    """Отправка HTTP-запроса к сервису с повторными попытками и проверкой статуса."""
    url = SERVICE_URLS.get(service)
    if not url:
        logger.error(f"Неизвестный сервис: {service}")
        return {"status": "error", "message": f"Неизвестный сервис: {service}"}

    # Устанавливаем таймаут: 120 секунд для gibdd_auto и gibdd_fines, 30 секунд для остальных
    timeout = 120 if service in ["gibdd_auto", "gibdd_fines"] else 30
    total_timeout = timeout * max_attempts  # Общий таймаут для всех попыток

    async with aiohttp.ClientSession() as session:
        start_time = asyncio.get_event_loop().time()
        attempt = 1

        while attempt <= max_attempts:
            try:
                async with session.post(url, json=payload, timeout=timeout) as response:
                    if response.status != 200:
                        error_msg = f"Ошибка {response.status} от сервиса {service}"
                        logger.error(error_msg)
                        if attempt == max_attempts:
                            return {"status": "error", "message": error_msg}
                        logger.info(f"Попытка {attempt} не удалась для {service}, повтор через 5 секунд")
                        await asyncio.sleep(5)
                        attempt += 1
                        continue

                    data = await response.json()
                    logger.info(f"Запрос к {service}: {data}")

                    # Проверяем, является ли ответ промежуточным для gibdd_auto и gibdd_fines
                    if service in ["gibdd_auto", "gibdd_fines"]:
                        # Для gibdd_fines: промежуточный ответ
                        if data.get("data") == "Выполняется запрос, ждите...":
                            elapsed_time = asyncio.get_event_loop().time() - start_time
                            if elapsed_time >= total_timeout:
                                logger.error(f"Превышен общий таймаут ({total_timeout} секунд) для {service}")
                                return {"status": "error", "message": f"Превышен таймаут {total_timeout} секунд"}
                            logger.info(f"Промежуточный ответ от {service}, ожидание {check_interval} секунд перед повторной проверкой")
                            await asyncio.sleep(check_interval)
                            continue
                        # Для gibdd_auto: ошибка таймаута или retry=True
                        elif service == "gibdd_auto" and (
                            data.get("status") == "error" and (
                                "Timeout 10000ms exceeded" in data.get("message", "") or
                                data.get("retry", False)
                            )
                        ):
                            elapsed_time = asyncio.get_event_loop().time() - start_time
                            if elapsed_time >= total_timeout:
                                logger.error(f"Превышен общий таймаут ({total_timeout} секунд) для {service}")
                                return {"status": "error", "message": f"Превышен таймаут {total_timeout} секунд"}
                            logger.info(f"Ошибка с возможностью повтора от {service}, ожидание {check_interval} секунд перед повторной проверкой")
                            await asyncio.sleep(check_interval)
                            continue

                    # Если ответ содержит финальный статус, возвращаем его
                    if data.get("status") in ["success", "error", "no_data"]:
                        logger.info(f"Успешный запрос к {service}: {data}")
                        return data
                    else:
                        logger.warning(f"Некорректный ответ от {service}: {data}")
                        if attempt == max_attempts:
                            return {"status": "error", "message": f"Некорректный ответ от {service}"}
                        await asyncio.sleep(5)
                        attempt += 1

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.error(f"Ошибка при запросе к {service} (попытка {attempt}): {str(e)}")
                if attempt == max_attempts:
                    return {"status": "error", "message": f"Ошибка после {max_attempts} попыток: {str(e)}"}
                logger.info(f"Попытка {attempt} не удалась для {service}, повтор через 5 секунд")
                await asyncio.sleep(5)
                attempt += 1

        return {"status": "error", "message": f"Не удалось получить финальный ответ от {service} после {max_attempts} попыток"}

async def update_db_records(context: ContextTypes.DEFAULT_TYPE):
    """Ежедневное обновление записей в базе данных."""
    logger.info("Начало ежедневного обновления записей в базе данных")
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT inn, fio, vin, sts, grz FROM debtors')
        records = cursor.fetchall()

    for record in records:
        inn, fio, vin, sts, grz = record
        new_data = {
            "inn": inn,
            "fio": fio,
            "vin": vin,
            "sts": sts,
            "grz": grz,
            "gibdd_auto": "",
            "gibdd_fines": "",
            "efrsb": "",
            "nsis": "",
            "reestr_zalogov": "",
            "notariat": "",
            "pb_nalog": "",
            "kad_arbitr": ""
        }

        try:
            if vin:
                new_data["gibdd_auto"] = json.dumps(await fetch_service_data("gibdd_auto", {"vin": vin}),
                                                    ensure_ascii=False)
                new_data["nsis"] = json.dumps(await fetch_service_data("osago", {"vin": vin}), ensure_ascii=False)
                new_data["reestr_zalogov"] = json.dumps(await fetch_service_data("pledge", {"vin": vin}),
                                                        ensure_ascii=False)
            if grz and sts:
                grz_parts = grz.split()
                if len(grz_parts) == 2:
                    regnum, regreg = grz_parts
                    new_data["gibdd_fines"] = json.dumps(await fetch_service_data("gibdd_fines", {
                        "regnum": regnum,
                        "regreg": regreg,
                        "stsnum": sts
                    }), ensure_ascii=False)
                else:
                    new_data["gibdd_fines"] = json.dumps(
                        {"status": "error", "message": "Неверный формат ГРЗ"}, ensure_ascii=False)
            if inn:
                new_data["efrsb"] = json.dumps(await fetch_service_data("efrsb", {"inn": inn}), ensure_ascii=False)
                new_data["pb_nalog"] = json.dumps(await fetch_service_data("nalog", {"inn": inn}), ensure_ascii=False)
                new_data["kad_arbitr"] = json.dumps(await fetch_service_data("kad_arbitr", {"inn": inn}),
                                                    ensure_ascii=False)
            if fio:
                parts = fio.split(';')
                if len(parts) == 2:
                    name, birth_date = parts
                    new_data["notariat"] = json.dumps(await fetch_service_data("probate", {
                        "name": name.strip(),
                        "birth_date": birth_date.strip()
                    }), ensure_ascii=False)
                else:
                    new_data["notariat"] = json.dumps(await fetch_service_data("probate", {
                        "name": fio.strip(),
                        "birth_date": ""
                    }), ensure_ascii=False)
        except Exception as e:
            logger.error(f"Ошибка при обновлении данных для ИНН {inn}: {e}")
            for field in ["gibdd_auto", "gibdd_fines", "efrsb", "nsis", "reestr_zalogov", "notariat", "pb_nalog",
                          "kad_arbitr"]:
                if not new_data[field]:
                    new_data[field] = json.dumps({"status": "error", "message": str(e)}, ensure_ascii=False)

        current_data = get_from_db(inn)
        changes = {}
        for field in ['gibdd_auto', 'gibdd_fines', 'efrsb', 'nsis', 'reestr_zalogov', 'notariat', 'pb_nalog',
                      'kad_arbitr']:
            if current_data[field] != new_data[field]:
                changes[field] = (current_data[field], new_data[field])

        if changes:
            save_to_db(new_data)
            log_updates(inn, changes)
            logger.info(f"Обновлены данные для ИНН {inn}")
        else:
            logger.info(f"Изменений для ИНН {inn} не обнаружено")


def validate_input(data: dict) -> tuple[bool, str]:
    """Валидация входных данных."""
    if "inn" in data and not re.match(r'^\d{10}$|^\d{12}$', data["inn"]):
        return False, "ИНН должен содержать 10 или 12 цифр."
    if "fio" in data:
        if not re.match(r'^[А-Яа-я\s-]{2,}\s+[А-Яа-я\s-]+(?:;[0-3]\d\.[0-1]\d\.\d{4})?$', data["fio"]):
            return False, "ФИО должно содержать минимум имя и фамилию, или ФИО;дата_рождения (например, Иванов Иван;01.01.1970)."
    if "vin" in data:
        vin = data["vin"]
        logger.debug(f"Валидация VIN: {vin}, длина: {len(vin)}")
        if not re.match(r'^[A-Z0-9]{17}$', vin):
            return False, f"VIN должен содержать ровно 17 символов (цифры и латинские). Получено: {vin}"
    if "sts" in data and not re.match(r'^\d{2}[А-Я0-9]{2}\d{6}$', data["sts"]):
        return False, "СТС должен быть в формате: (например, 99АА999999)."
    if "grz" in data and not re.match(r'^[А-Я]\d{3}[А-Я]{2}\s\d{2,3}$', data["grz"]):
        return False, "ГРЗ должен быть в формате: буква, 3 цифры, 2 буквы, пробел, 2-3 цифры (например, А123БВ 777). Буквы кириллица"
    return True, ""


def save_to_db(data: dict):
    """Сохранение данных в базу."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO debtors (
                inn, fio, vin, sts, grz, gibdd_auto, gibdd_fines, efrsb, nsis, reestr_zalogov, notariat, pb_nalog, kad_arbitr
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data.get("inn"), data.get("fio"), data.get("vin"), data.get("sts"), data.get("grz"),
            data.get("gibdd_auto"), data.get("gibdd_fines"), data.get("efrsb"), data.get("nsis"),
            data.get("reestr_zalogov"), data.get("notariat"), data.get("pb_nalog"), data.get("kad_arbitr")
        ))
        conn.commit()


def update_db_field(inn: str, field: str, value: str):
    """Обновление одного поля в базе данных."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(f'UPDATE debtors SET {field} = ? WHERE inn = ?', (value, inn))
        conn.commit()


def get_from_db(inn: str) -> dict:
    """Поиск данных в базе по ИНН."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM debtors WHERE inn = ?', (inn,))
        row = cursor.fetchone()
        if row:
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))
        return {
            "inn": "", "fio": "", "vin": "", "sts": "", "grz": "",
            "gibdd_auto": "", "gibdd_fines": "", "efrsb": "", "nsis": "",
            "reestr_zalogov": "", "notariat": "", "pb_nalog": "", "kad_arbitr": ""
        }


async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать основные кнопки меню."""
    keyboard = [
        [InlineKeyboardButton("Создание портрета должника", callback_data="create_portrait")],
        [InlineKeyboardButton("Данные обработанных должников", callback_data="view_data")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    for attempt in range(3):
        try:
            if update.callback_query:
                await update.callback_query.message.reply_text("Выберите действие:", reply_markup=reply_markup)
            else:
                await update.message.reply_text("Выберите действие:", reply_markup=reply_markup)

            return
        except TimedOut:
            logger.warning(f"Тайм-аут при отправке главного меню (попытка {attempt + 1}/3).")
            await asyncio.sleep(2)
    logger.error("Не удалось отправить главное меню после 3 попыток.")
    if update.callback_query:
        await update.callback_query.message.reply_text("Ошибка связи с сервером. Попробуйте снова.")
    else:
        await update.message.reply_text("Ошибка связи с сервером. Попробуйте снова.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start."""
    logger.info(f"Пользователь {update.effective_user.id} запустил команду /start")
    try:
        await update.message.reply_text(
            "Уважаемый пользователь,\n\n"
            "Я бот, который помогает создать портрет должника. Выберите действие ниже:"
        )
        await show_main_menu(update, context)
    except TimedOut:
        logger.warning("Тайм-аут при выполнении команды /start. Пробуем снова.")
        await asyncio.sleep(2)
        await update.message.reply_text(
            "Уважаемый пользователь,\n\n"
            "Я бот, который помогает создать портрет должника. Выберите действие ниже:"
        )
        await show_main_menu(update, context)


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатий на кнопки."""
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = update.effective_user.id

    try:
        if data == "create_portrait":
            context.user_data["state"] = "collect_input"
            context.user_data["input_data"] = {}
            await query.message.reply_text(
                "Введите данные построчно в формате:\n"
                "ИНН: <значение>\n"
                "ФИО: <значение>\n"
                "VIN: <значение>\n"
                "СТС: <значение>\n"
                "ГРЗ: <значение>\n"
                "После ввода всех данных нажмите 'Отправить'.\n"
                "Пример:\n"
                "ИНН: 671311855235\n"
                "ФИО: Иванов Иван Иванович;15.02.1996\n"
                "VIN: JN1TTNJ52U0650947\n"
                "СТС: 99АА999999\n"
                "ГРЗ: А123БВ 777"
            )
            if os.path.exists(EXAMPLE_FILE):
                with open(EXAMPLE_FILE, 'rb') as f:
                    await query.message.reply_document(document=f, filename='Пример.xlsx')
            else:
                await query.message.reply_text("Ошибка: файл Пример.xlsx не найден в папке проекта.")
        elif data == "submit_input":
            context.user_data["state"] = "create_portrait"
            input_data = context.user_data.get("input_data", {})
            await process_collected_input(input_data, update, context)
        elif data == "view_data":
            context.user_data["state"] = "awaiting_inn"
            await query.message.reply_text("Введите ИНН должника:")
        elif data == "edit_data":
            context.user_data["state"] = "select_field"
            context.user_data["field_page"] = 0
            await show_field_buttons(query, context)
        elif data.startswith("field_"):
            field = data.split("_")[1]
            context.user_data["state"] = f"edit_{field}"
            await query.message.reply_text(f"Введите новое значение для поля {field}:")
        elif data == "next_fields":
            context.user_data["field_page"] += 1
            await show_field_buttons(query, context)
        elif data == "prev_fields":
            context.user_data["field_page"] -= 1
            await show_field_buttons(query, context)
        elif data == "back_to_menu":
            await show_main_menu(update, context)
    except TimedOut:
        logger.warning(f"Тайм-аут при обработке кнопки {data}. Пробуем снова.")
        await asyncio.sleep(2)
        await query.message.reply_text("Произошла ошибка связи. Пожалуйста, выберите действие снова.")
        await show_main_menu(update, context)

async def show_field_buttons(query, context: ContextTypes.DEFAULT_TYPE):
    """Показать кнопки для выбора поля с пагинацией."""
    page = context.user_data.get("field_page", 0)
    start_idx = page * FIELDS_PER_PAGE
    end_idx = start_idx + FIELDS_PER_PAGE
    fields_to_show = EDITABLE_FIELDS[start_idx:end_idx]

    keyboard = [[InlineKeyboardButton(field, callback_data=f"field_{field}")] for field in fields_to_show]
    nav_buttons = []
    if start_idx > 0:
        nav_buttons.append(InlineKeyboardButton("Назад", callback_data="prev_fields"))
    if end_idx < len(EDITABLE_FIELDS):
        nav_buttons.append(InlineKeyboardButton("Далее", callback_data="next_fields"))
    if nav_buttons:
        keyboard.append(nav_buttons)

    reply_markup = InlineKeyboardMarkup(keyboard)
    for attempt in range(3):
        try:
            await query.message.reply_text("Выберите поле для редактирования:", reply_markup=reply_markup)
            return
        except TimedOut:
            logger.warning(f"Тайм-аут при отправке кнопок полей (попытка {attempt + 1}/3).")
            await asyncio.sleep(2)
    logger.error("Не удалось отправить кнопки полей после 3 попыток.")
    await query.message.reply_text("Ошибка связи с сервером. Попробуйте снова.")


async def process_collected_input(data: dict, update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка собранных данных: постановка в очередь или обработка из очереди."""
    # Этап 1: Постановка в очередь
    is_valid, error_msg = validate_input(data)
    if not is_valid:
        await update.callback_query.message.reply_text(f"Ошибка: {error_msg}")
        return

    if not data.get("inn"):
        await update.callback_query.message.reply_text("Ошибка: необходимо указать хотя бы ИНН.")
        return

    # Ограничение размера очереди
    if request_queue.qsize() >= 10:
        logger.warning(f"Очередь переполнена для ИНН {data.get('inn')}")
        await update.callback_query.message.reply_text("Очередь переполнена. Пожалуйста, попробуйте позже.")
        return

    # Добавление запроса в очередь
    await request_queue.put((data, update, False))
    queue_size = request_queue.qsize()
    logger.info(f"Запрос для ИНН {data.get('inn')} добавлен в очередь. Размер очереди: {queue_size}")
    await update.callback_query.message.reply_text(
        f"Ваш запрос принят. В очереди {queue_size} запрос(ов). Пожалуйста, ожидайте."
    )
    return

async def process_single_request(data: dict, update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка одного запроса из очереди."""
    inn = data.get("inn", "")
    waiting_message = await update.callback_query.message.reply_text(
        f"Обработка ИНН {inn}. Пожалуйста, ожидайте."
    )

    try:
        collected_data = {
            "inn": data.get("inn", ""),
            "fio": data.get("fio", ""),
            "vin": data.get("vin", ""),
            "sts": data.get("sts", ""),
            "grz": data.get("grz", ""),
            "gibdd_auto": "",
            "gibdd_fines": "",
            "efrsb": "",
            "nsis": "",
            "reestr_zalogov": "",
            "notariat": "",
            "pb_nalog": "",
            "kad_arbitr": ""
        }

        async with aiohttp.ClientSession() as session:
            tasks = []
            if collected_data["vin"]:
                tasks.extend([
                    fetch_service_data("gibdd_auto", {"vin": collected_data["vin"]}),
                    fetch_service_data("osago", {"vin": collected_data["vin"]}),
                    fetch_service_data("pledge", {"vin": collected_data["vin"]})
                ])
            if collected_data["grz"] and collected_data["sts"]:
                grz_parts = collected_data["grz"].split()
                if len(grz_parts) == 2:
                    regnum, regreg = grz_parts
                    tasks.append(fetch_service_data("gibdd_fines", {
                        "regnum": regnum,
                        "regreg": regreg,
                        "stsnum": collected_data["sts"]
                    }))
                else:
                    collected_data["gibdd_fines"] = json.dumps(
                        {"status": "error", "message": "Неверный формат ГРЗ"}, ensure_ascii=False)
            if collected_data["inn"]:
                tasks.extend([
                    fetch_service_data("efrsb", {"inn": collected_data["inn"]}),
                    fetch_service_data("nalog", {"inn": collected_data["inn"]}),
                    fetch_service_data("kad_arbitr", {"inn": collected_data["inn"]})
                ])
            if collected_data["fio"]:
                parts = collected_data["fio"].split(';')
                if len(parts) == 2:
                    name, birth_date = parts
                    tasks.append(fetch_service_data("probate", {
                        "name": name.strip(),
                        "birth_date": birth_date.strip()
                    }))
                else:
                    tasks.append(fetch_service_data("probate", {
                        "name": collected_data["fio"].strip(),
                        "birth_date": ""
                    }))

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
            else:
                results = []

            task_idx = 0
            if collected_data["vin"]:
                collected_data["gibdd_auto"] = json.dumps(results[task_idx], ensure_ascii=False)
                collected_data["nsis"] = json.dumps(results[task_idx + 1], ensure_ascii=False)
                collected_data["reestr_zalogov"] = json.dumps(results[task_idx + 2], ensure_ascii=False)
                task_idx += 3
            if collected_data["grz"] and collected_data["sts"] and len(collected_data["grz"].split()) == 2:
                collected_data["gibdd_fines"] = json.dumps(results[task_idx], ensure_ascii=False)
                task_idx += 1
            if collected_data["inn"]:
                collected_data["efrsb"] = json.dumps(results[task_idx], ensure_ascii=False)
                collected_data["pb_nalog"] = json.dumps(results[task_idx + 1], ensure_ascii=False)
                collected_data["kad_arbitr"] = json.dumps(results[task_idx + 2], ensure_ascii=False)
                task_idx += 3
            if collected_data["fio"]:
                collected_data["notariat"] = json.dumps(results[task_idx], ensure_ascii=False)

        save_to_db(collected_data)

        data_summary = (
            f"ИНН: {collected_data['inn'] or 'Не указано'}\n"
            f"ФИО: {collected_data['fio'] or 'Не указано'}\n"
            f"VIN: {collected_data['vin'] or 'Не указано'}\n"
            f"СТС: {collected_data['sts'] or 'Не указано'}\n"
            f"ГРЗ: {collected_data['grz'] or 'Не указано'}\n"
            f"ГИБДД (авто): {collected_data['gibdd_auto'] or 'Данные отсутствуют'}\n"
            f"ГИБДД (штрафы): {collected_data['gibdd_fines'] or 'Данные отсутствуют'}\n"
            f"ЕФРСБ: {collected_data['efrsb'] or 'Данные отсутствуют'}\n"
            f"НСИС: {collected_data['nsis'] or 'Данные отсутствуют'}\n"
            f"Реестр залогов: {collected_data['reestr_zalogov'] or 'Данные отсутствуют'}\n"
            f"Нотариат: {collected_data['notariat'] or 'Данные отсутствуют'}\n"
            f"ПБ Налог: {collected_data['pb_nalog'] or 'Данные отсутствуют'}\n"
            f"Кад.арбитр: {collected_data['kad_arbitr'] or 'Данные отсутствуют'}"
        )

        chat_completion = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Проанализируйте данные о должнике и составьте портрет:\n{data_summary}"}
            ]
        )
        response = chat_completion.choices[0].message.content
        logger.info(f"Ответ от DeepSeek для ИНН {inn} получен")

        await waiting_message.delete()
        waiting_message = None  # Устанавливаем в None после удаления
        for attempt in range(3):
            try:
                await update.callback_query.message.reply_text(f"Портрет должника (ИНН {inn}):\n\n{response}")
                await show_main_menu(update, context)
                break
            except TimedOut:
                logger.warning(f"Тайм-аут при отправке портрета для ИНН {inn} (попытка {attempt + 1}/3)")
                await asyncio.sleep(2)
        else:
            logger.error(f"Не удалось отправить портрет для ИНН {inn} после 3 попыток")
            await update.callback_query.message.reply_text("Ошибка связи с сервером. Пожалуйста, попробуйте снова.")
            await show_main_menu(update, context)

    except Exception as e:
        logger.error(f"Ошибка при обработке ИНН {inn}: {str(e)}")
        if waiting_message:
            try:
                await waiting_message.delete()
            except telegram.error.BadRequest as br:
                logger.warning(f"Не удалось удалить сообщение: {br}")
        await update.callback_query.message.reply_text(f"Ошибка обработки ИНН {inn}: {str(e)}")
        await show_main_menu(update, context)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений и файлов."""
    user_id = update.effective_user.id
    state = context.user_data.get("state")

    if not update.message:
        logger.error("Получен update без message")
        return

    if state == "collect_input":
        user_input = update.message.text.strip()
        lines = user_input.split('\n')
        input_data = context.user_data.get("input_data", {})

        for line in lines:
            line = line.strip()
            if not line:
                continue
            if ":" not in line:
                await update.message.reply_text("Ошибка: каждая строка должна содержать двоеточие ':'.")
                return
            key, value = [part.strip() for part in line.split(":", 1)]
            if not value:
                await update.message.reply_text(f"Ошибка: значение для поля '{key}' не указано.")
                return
            key = key.lower()

            key_map = {
                "инн": "inn",
                "фио": "fio",
                "vin": "vin",
                "стс": "sts",
                "грз": "grz"
            }
            if key not in key_map:
                await update.message.reply_text(
                    f"Ошибка: неизвестное поле '{key}'. Допустимые поля: ИНН, ФИО, VIN, СТС, ГРЗ.")
                return

            input_data[key_map[key]] = value
            is_valid, error_message = validate_input({key_map[key]: value})
            if not is_valid:
                await update.message.reply_text(f"Ошибка в данных: {error_message}")
                return

        context.user_data["input_data"] = input_data
        reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("Отправить", callback_data="submit_input")]])
        await update.message.reply_text("Данные приняты. Введите следующее поле или нажмите 'Отправить'.",
                                        reply_markup=reply_markup)
        return

    if state and state.startswith("edit_"):
        field = state.split("_")[1]
        new_value = update.message.text.strip()
        inn = context.user_data.get("current_inn")

        is_valid, error_msg = validate_input({field: new_value})
        if field in ["inn", "fio", "vin", "sts", "grz"] and not is_valid:
            try:
                await update.message.reply_text(f"Ошибка: {error_msg}")
            except TimedOut:
                logger.warning("Тайм-аут при отправке ошибки валидации. Пробуем снова.")
                await asyncio.sleep(2)
                await update.message.reply_text(f"Ошибка: {error_msg}")
            return

        current_data = get_from_db(inn)
        if current_data:
            changes = {field: (current_data[field], new_value)}
            log_updates(inn, changes)

        update_db_field(inn, field, new_value)
        try:
            await update.message.reply_text(f"Поле {field} успешно обновлено.")
            await show_main_menu(update, context)
        except TimedOut:
            logger.warning("Тайм-аут при отправке подтверждения обновления. Пробуем снова.")
            await asyncio.sleep(2)
            await update.message.reply_text(f"Поле {field} успешно обновлено.")
            await show_main_menu(update, context)
        return

    if state == "awaiting_inn":
        inn = update.message.text.strip()
        is_valid, error_msg = validate_input({"inn": inn})
        if not is_valid:
            try:
                await update.message.reply_text(f"Ошибка: {error_msg}")
            except TimedOut:
                logger.warning("Тайм-аут при отправке ошибки валидации ИНН. Пробуем снова.")
                await asyncio.sleep(2)
                await update.message.reply_text(f"Ошибка: {error_msg}")
            return

        db_data = get_from_db(inn)
        if db_data and db_data.get("inn"):
            fio = db_data.get('fio', 'Не указано')
            birth_date = ""
            if ';' in fio:
                fio, birth_date = fio.split(';', 1)
                birth_date = birth_date.strip() or "Не указано"
            fio = fio.strip() or "Не указано"

            def format_field(field_data, field_name, input_data=None):
                logger.debug(f"Processing field: {field_name}, data type: {type(field_data)}, data: {field_data}")

                if field_data is None or (isinstance(field_data, str) and not field_data.strip()):
                    if input_data and field_name in ["gibdd_auto", "gibdd_fines", "nsis", "reestr_zalogov"]:
                        logger.debug(f"Skipping {field_name} due to missing input data")
                        return None
                    if input_data and field_name in ["efrsb", "pb_nalog", "kad_arbitr"]:
                        logger.debug(f"Skipping {field_name} due to missing INN")
                        return None
                    if input_data and field_name == "notariat":
                        logger.debug(f"Skipping {field_name} due to missing FIO")
                        return None
                    logger.debug(f"No data for {field_name}, returning default")
                    return "- Статус: Не найдено"

                try:
                    if isinstance(field_data, str):
                        stripped_data = field_data.strip()
                        if not stripped_data:
                            logger.debug(f"Empty string for {field_name}")
                            return "- Статус: Не найдено"
                        try:
                            data = json.loads(stripped_data)
                            logger.debug(f"JSON parse for {field_name}: type={type(data)}, data={data}")
                        except json.JSONDecodeError as e:
                            logger.error(f"JSON decode error for {field_name}: {e}, data: {stripped_data}")
                            return f"- Статус: Ошибка: Некорректный формат JSON"
                    elif isinstance(field_data, dict):
                        data = field_data
                        logger.debug(f"Data for {field_name} is already a dict: {data}")
                    else:
                        logger.error(f"Invalid data type for {field_name}: {type(field_data)}, data: {field_data}")
                        return f"- Статус: Ошибка: Неподдерживаемый тип данных {type(field_data)}"

                    if not isinstance(data, dict):
                        logger.error(
                            f"Parsed data for {field_name} is not a dictionary: {type(data)}, data: {data}")
                        return f"- Статус: Ошибка: Данные не являются словарем"

                    if field_name == "gibdd_auto":
                        result = []
                        if data.get("status") == "success":
                            vehicle = data.get("vehicle", {})
                            ownership_periods = data.get("ownership_periods", [])
                            status = vehicle.get("статус_записи", "Неизвестно")
                            record_id = vehicle.get("уникальный_номер_записи", "Неизвестно")
                            make_model = vehicle.get("марка_и(или)_модель", "Неизвестно")
                            year = vehicle.get("год_выпуска", "Неизвестно")
                            vin = vehicle.get("идентификационный_номер_(vin)", "Неизвестно")
                            chassis = vehicle.get("номер_шасси_(рамы)", "Неизвестно")
                            body = vehicle.get("номер_кузова_(кабины)", "Неизвестно")
                            color = vehicle.get("цвет_кузова_(кабины)", "Неизвестно")
                            engine = vehicle.get("номер_двигателя", "Неизвестно")
                            displacement = vehicle.get("рабочий_объем_(см³)", "Неизвестно")
                            power = vehicle.get("мощность_(квт/л.с.)", "Неизвестно")
                            eco_class = vehicle.get("экологический_класс", "Неизвестно")
                            vehicle_type = vehicle.get("тип_транспортного_средства", "Неизвестно")

                            result.append(f"- История ТС: {make_model} ({year})")
                            result.append(f"  - Статус записи: {status}")
                            result.append(f"  - Уникальный номер записи: {record_id}")
                            result.append(f"  - VIN: {vin}")
                            result.append(f"  - Номер шасси (рамы): {chassis}")
                            result.append(f"  - Номер кузова (кабины): {body}")
                            result.append(f"  - Цвет кузова: {color}")
                            result.append(f"  - Номер двигателя: {engine}")
                            result.append(f"  - Рабочий объем: {displacement} см³")
                            result.append(f"  - Мощность: {power}")
                            result.append(f"  - Экологический класс: {eco_class}")
                            result.append(f"  - Тип ТС: {vehicle_type}")

                            if ownership_periods:
                                result.append("  - Периоды владения: " + ", ".join(
                                    [
                                        f"{period.get('from', 'Неизвестно')} - {period.get('to', 'по н.в.')} ({period.get('owner_type', 'Неизвестно')})"
                                        for period in ownership_periods]))
                            else:
                                result.append("  - Периоды владения: Не найдено")
                            result.append("- Статус: Успешное получение данных")
                        else:
                            error_msg = data.get("message", "Неизвестная ошибка")
                            logger.warning(f"Error in gibdd_auto: {error_msg}, data: {data}")
                            result.append("- История ТС: Не найдено")
                            result.append(f"- Статус: Ошибка: {error_msg}")
                        return "\n".join(result)

                    elif field_name == "gibdd_fines":
                        result = []
                        if data.get("status") == "success":
                            fines = data.get("fines", [])
                            if not fines:
                                result.append("- Штрафы: Не найдено")
                            else:
                                for fine in fines:
                                    result.append(f"- Штраф:")
                                    result.append(f"  - Дата: {fine.get('date', 'Неизвестно')}")
                                    result.append(f"  - Сумма: {fine.get('amount', 'Неизвестно')}")
                                    result.append(f"  - Нарушение: {fine.get('violation', 'Неизвестно')}")
                            result.append("- Статус: Успешное получение данных")
                        else:
                            error_msg = data.get("message", "Неизвестная ошибка")
                            logger.warning(f"Error in gibdd_fines: {error_msg}, data: {data}")
                            result.append("- Штрафы: Не найдено")
                            result.append(f"- Статус: Ошибка: {error_msg}")
                        return "\n".join(result)

                    elif field_name == "nsis":
                        result = []
                        if data.get("status") == "success":
                            policies = data.get("policies", [])
                            if not policies:
                                result.append("- Полисы ОСАГО: Не найдено")
                            else:
                                for policy in policies:
                                    result.append(f"- Полис:")
                                    result.append(f"  - Номер: {policy.get('policy_number', 'Неизвестно')}")
                                    result.append(f"  - Страховая: {policy.get('insurer', 'Неизвестно')}")
                                    result.append(f"  - Действует с: {policy.get('valid_from', 'Неизвестно')}")
                                    result.append(f"  - Действует до: {policy.get('valid_to', 'Неизвестно')}")
                            result.append("- Статус: Успешное получение данных")
                        else:
                            error_msg = data.get("message", "Неизвестная ошибка")
                            result.append("- Полисы ОСАГО: Не найдено")
                            result.append(f"- Статус: Ошибка: {error_msg}")
                        return "\n".join(result)

                    elif field_name == "reestr_zalogov":
                        result = []
                        if data.get("status") == "success":
                            pledges = data.get("details", [])
                            if not pledges:
                                result.append("- Залоги: Не найдено")
                            else:
                                for pledge in pledges:
                                    result.append(f"- Залог:")
                                    result.append(f"  - Номер дела: {pledge.get('case_number', 'Неизвестно')}")
                                    result.append(f"  - Дата: {pledge.get('date', 'Неизвестно')}")
                                    result.append(f"  - Залогодатель: {pledge.get('pledgor', 'Неизвестно')}")
                            result.append("- Статус: Успешное получение данных")
                        else:
                            error_msg = data.get("message", "Неизвестная ошибка")
                            result.append("- Залоги: Не найдено")
                            result.append(f"- Статус: Ошибка: {error_msg}")
                        return "\n".join(result)

                    elif field_name == "efrsb":
                        result = []
                        if data.get("status") == "success":
                            individuals = data.get("individuals", [])
                            legal_entities = data.get("legal_entities", [])
                            if not (individuals or legal_entities):
                                result.append("- Банкротство: Не найдено")
                            else:
                                if individuals:
                                    for person in individuals:
                                        result.append(f"- Физическое лицо:")
                                        result.append(f"  - ФИО: {person.get('full_name', 'Неизвестно')}")
                                        result.append(f"  - Статус: {person.get('status', 'Неизвестно')}")
                                        result.append(
                                            f"  - Номер дела: {person.get('court_case_number', 'Неизвестно')}")
                                if legal_entities:
                                    for entity in legal_entities:
                                        result.append(f"- Юридическое лицо:")
                                        result.append(f"  - Название: {entity.get('name', 'Неизвестно')}")
                                        result.append(f"  - Статус: {entity.get('status', 'Неизвестно')}")
                                        result.append(
                                            f"  - Номер дела: {entity.get('court_case_number', 'Неизвестно')}")
                            result.append("- Статус: Успешное получение данных")
                        else:
                            error_msg = data.get("message", "Неизвестная ошибка")
                            result.append("- Банкротство: Не найдено")
                            result.append(f"- Статус: Ошибка: {error_msg}")
                        return "\n".join(result)

                    elif field_name == "pb_nalog":
                        result = []
                        if data.get("status") == "success":
                            results = data.get("results", {})
                            for title, info in results.items():
                                result.append(f"- {title}:")
                                for item in info.get("data", []):
                                    for key, value in item.items():
                                        result.append(f"  - {key}: {value}")
                            result.append("- Статус: Успешное получение данных")
                        else:
                            error_msg = data.get("message", "Неизвестная ошибка")
                            result.append("- Налоговая информация: Не найдено")
                            result.append(f"- Статус: Ошибка: {error_msg}")
                        return "\n".join(result)

                    elif field_name == "kad_arbitr":
                        result = []
                        if data.get("status") == "success":
                            cases = data.get("cases", [])
                            if not cases:
                                result.append("- Судебные дела: Не найдено")
                            else:
                                for case in cases:
                                    result.append(f"- Дело:")
                                    result.append(f"  - Номер: {case.get('case_number', 'Неизвестно')}")
                                    result.append(f"  - Дата: {case.get('registration_date', 'Неизвестно')}")
                                    result.append(f"  - Истец: {case.get('plaintiff', 'Неизвестно')}")
                                    result.append(f"  - Ответчик: {case.get('respondent', 'Неизвестно')}")
                            result.append("- Статус: Успешное получение данных")
                        else:
                            error_msg = data.get("message", "Неизвестная ошибка")
                            result.append("- Судебные дела: Не найдено")
                            result.append(f"- Статус: Ошибка: {error_msg}")
                        return "\n".join(result)

                    elif field_name == "notariat":
                        result = []
                        if data.get("status") == "success":
                            case = data.get("case", {})
                            if not case:
                                result.append("- Наследственные дела: Не найдено")
                            else:
                                result.append(f"- Наследственное дело:")
                                result.append(f"  - ФИО: {case.get('name', 'Неизвестно')}")
                                result.append(f"  - Дата рождения: {case.get('birth_date', 'Неизвестно')}")
                                result.append(f"  - Записи: {case.get('probate-cases__records', 'Неизвестно')}")
                            result.append("- Статус: Успешное получение данных")
                        else:
                            error_msg = data.get("message", "Неизвестная ошибка")
                            result.append("- Наследственные дела: Не найдено")
                            result.append(f"- Статус: Ошибка: {error_msg}")
                        return "\n".join(result)

                    return f"- Статус: {data}"
                except json.JSONDecodeError:
                    return "- Статус: Некорректные данные"

            report = [f"Отчет по должнику (ИНН: {inn})", "============================="]
            report.append("\n1. Основные данные")
            report.append("-------------------")
            report.append(f"- ФИО: {fio}")
            report.append(f"- Дата рождения: {birth_date}")
            report.append(f"- ИНН: {inn}")
            vin = db_data.get('vin', 'Не предоставлено')
            sts = db_data.get('sts', 'Не предоставлено')
            grz = db_data.get('grz', 'Не предоставлено')
            if vin != 'Не предоставлено':
                report.append(f"- VIN: {vin}")
            if sts != 'Не предоставлено':
                report.append(f"- СТС: {sts}")
            if grz != 'Не предоставлено':
                report.append(f"- Госномер: {grz}")

            if vin != 'Не предоставлено' or sts != 'Не предоставлено' or grz != 'Не предоставлено':
                report.append("\n2. ГИБДД")
                report.append("-------------------")
                gibdd_auto = format_field(db_data.get('gibdd_auto'), 'gibdd_auto', {'vin': vin})
                if gibdd_auto:
                    report.append(gibdd_auto)
                gibdd_fines = format_field(db_data.get('gibdd_fines'), 'gibdd_fines', {'sts': sts, 'grz': grz})
                if gibdd_fines:
                    report.append(gibdd_fines)

            if vin != 'Не предоставлено':
                nsis = format_field(db_data.get('nsis'), 'nsis', {'vin': vin})
                if nsis:
                    report.append("\n3. ОСАГО")
                    report.append("-------------------")
                    report.append(nsis)

            if vin != 'Не предоставлено':
                reestr_zalogov = format_field(db_data.get('reestr_zalogov'), 'reestr_zalogov', {'vin': vin})
                if reestr_zalogov:
                    report.append("\n4. Реестр залогов")
                    report.append("-------------------")
                    report.append(reestr_zalogov)

            efrsb = format_field(db_data.get('efrsb'), 'efrsb', {'inn': inn})
            if efrsb:
                report.append("\n5. ЕФРСБ")
                report.append("-------------------")
                report.append(efrsb)

            pb_nalog = format_field(db_data.get('pb_nalog'), 'pb_nalog', {'inn': inn})
            if pb_nalog:
                report.append("\n6. Налоговые задолженности")
                report.append("-------------------")
                report.append(pb_nalog)

            kad_arbitr = format_field(db_data.get('kad_arbitr'), 'kad_arbitr', {'inn': inn})
            if kad_arbitr:
                report.append("\n7. Кад.арбитр")
                report.append("-------------------")
                report.append(kad_arbitr)

            notariat = format_field(db_data.get('notariat'), 'notariat', {'fio': fio})
            if notariat:
                report.append("\n8. Нотариат")
                report.append("-------------------")
                report.append(notariat)

            report.append("=============================")
            report = "\n".join(report)

            file_path = f"debtor_{inn}.txt"
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(report)
            except Exception as e:
                logger.error(f"Ошибка при создании файла отчета для ИНН {inn}: {e}")
                try:
                    await update.message.reply_text("Ошибка при создании отчета. Попробуйте снова.")
                    await show_main_menu(update, context)
                except TimedOut:
                    logger.warning("Тайм-аут при отправке ошибки создания отчета. Пробуем снова.")
                    await asyncio.sleep(2)
                    await update.message.reply_text("Ошибка при создании отчета. Попробуйте снова.")
                    await show_main_menu(update, context)
                return

            context.user_data["current_inn"] = inn
            keyboard = [
                [InlineKeyboardButton("Изменение данных", callback_data="edit_data")],
                [InlineKeyboardButton("Назад", callback_data="back_to_menu")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            for attempt in range(3):
                try:
                    with open(file_path, "rb") as f:
                        await update.message.reply_document(
                            document=f,
                            filename=f"debtor_{inn}.txt",
                            caption="Отчет по должнику"
                        )
                    await update.message.reply_text(
                        f"Отчет по должнику (ИНН: {inn}) отправлен.",
                        reply_markup=reply_markup
                    )
                    context.user_data["state"] = "view_data"
                    os.remove(file_path)
                    return
                except TimedOut:
                    logger.warning(f"Тайм-аут при отправке отчета (попытка {attempt + 1}/3).")
                    await asyncio.sleep(2)
                except Exception as e:
                    logger.error(f"Ошибка при отправке отчета для ИНН {inn}: {e}")
                    break

            logger.error("Не удалось отправить отчет после 3 попыток.")
            try:
                await update.message.reply_text("Ошибка связи с сервером. Пожалуйста, попробуйте снова позже.")
                await show_main_menu(update, context)
            except TimedOut:
                logger.warning("Тайм-аут при отправке ошибки. Пробуем снова.")
                await asyncio.sleep(2)
                await update.message.reply_text("Ошибка связи с сервером. Пожалуйста, попробуйте снова позже.")
                await show_main_menu(update, context)
            os.remove(file_path)
        else:
            try:
                await update.message.reply_text(f"Запись с ИНН {inn} не найдена в базе данных.")
                await show_main_menu(update, context)
            except TimedOut:
                logger.warning("Тайм-аут при отправке сообщения об отсутствии записи. Пробуем снова.")
                await asyncio.sleep(2)
                await update.message.reply_text(f"Запись с ИНН {inn} не найдена в базе данных.")
                await show_main_menu(update, context)
        return

    if state == "create_portrait" and hasattr(update.message,
                                              'document') and update.message.document and update.message.document.mime_type in [
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel"
    ]:
        logger.info(f"Пользователь {user_id} отправил Excel-файл")
        try:
            file = await update.message.document.get_file()
            file_path = f"temp_{uuid.uuid4()}.xlsx"
            await file.download_to_drive(file_path)

            df = pd.read_excel(file_path)
            expected_columns = ['ИНН', 'ФИО', 'VIN', 'СТС', 'ГРЗ']
            if list(df.columns) != expected_columns:
                await update.message.reply_text(
                    "Ошибка: файл должен содержать столбцы ИНН,ФИО,VIN,СТС,ГРЗ в первой строке."
                )
                os.remove(file_path)
                return

            total_records = len(df)
            await update.message.reply_text(f"Всего записей в файле: {total_records}")

            success_count = 0
            errors = []
            for index, row in df.iterrows():
                data = {
                    'inn': str(row['ИНН']).strip() if pd.notna(row['ИНН']) else '',
                    'fio': str(row['ФИО']).strip() if pd.notna(row['ФИО']) else '',
                    'vin': str(row['VIN']).strip() if pd.notna(row['VIN']) else '',
                    'sts': str(row['СТС']).strip() if pd.notna(row['СТС']) else '',
                    'grz': str(row['ГРЗ']).strip() if pd.notna(row['ГРЗ']) else ''
                }

                is_valid, error_msg = validate_input(data)
                if not is_valid:
                    errors.append(f"Строка {index + 2}: {error_msg}")
                    continue

                if request_queue.qsize() >= 10:
                    errors.append(f"Строка {index + 2}: Очередь переполнена")
                    continue

                await request_queue.put((data, update, True))
                success_count += 1
                queue_size = request_queue.qsize()
                logger.info(
                    f"Строка {index + 2} для ИНН {data['inn']} добавлена в очередь. Размер очереди: {queue_size}")
                await update.message.reply_text(
                    f"Строка {index + 2} добавлена в очередь. В очереди {queue_size} запрос(ов)."
                )

            os.remove(file_path)

            response = f"Обработка завершена.\nДобавлено в очередь: {success_count} строк.\n"
            if errors:
                response += "Ошибки:\n" + "\n".join(errors)
            await update.message.reply_text(response)
            await show_main_menu(update, context)

        except Exception as e:
            logger.error(f"Ошибка при обработке Excel-файла для пользователя {user_id}: {e}")
            await update.message.reply_text(f"Произошла ошибка при обработке файла: {e}. Попробуйте снова.")
            await show_main_menu(update, context)

    try:
        await update.message.reply_text("Пожалуйста, выберите действие из меню.")
        await show_main_menu(update, context)
    except TimedOut:
        logger.warning("Тайм-аут при отправке запроса на выбор действия. Пробуем снова.")
        await asyncio.sleep(2)
        await update.message.reply_text("Пожалуйста, выберите действие из меню.")
        await show_main_menu(update, context)


async def post_init(application: Application) -> None:
    """Запуск планировщика после инициализации бота."""
    scheduler.add_job(backup_db, 'interval', days=1)
    scheduler.add_job(update_db_records, 'cron', hour=0, minute=0, args=[None])
    scheduler.start()
    logger.info("Планировщик запущен")


async def post_stop(application: Application) -> None:
    """Остановка планировщика при завершении работы бота."""
    scheduler.shutdown()
    logger.info("Планировщик остановлен")


def main():
    """Запуск Telegram-бота."""
    try:
        init_db()
        application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
        application.add_handler(CommandHandler("start", start))
        application.add_handler(MessageHandler(filters.TEXT | filters.Document.ALL, handle_message))
        application.add_handler(CallbackQueryHandler(button_callback))
        application.post_init = post_init
        application.post_stop = post_stop

        # Запуск воркера
        loop = asyncio.get_event_loop()
        loop.create_task(worker(application.context_types.context))

        logger.info("Запуск бота...")
        print("Бот запущен")
        application.run_polling()

    except (KeyboardInterrupt, SystemExit):
        logger.info("Остановка бота...")
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}")
        print(f"Ошибка при запуске бота: {e}")
        exit(1)


if __name__ == '__main__':
    main()
import os
import re
import logging
import asyncio
import json
import time
import aiohttp
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from telegram.error import TimedOut

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных из .env файла
load_dotenv()

# Настройка токена
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')

# URLs сервисов
EFRSB_URL = "http://localhost:5001/efrsb"
KAD_ARBITR_URL = "http://localhost:5002/kad_arbitr"

# Очередь для обработки запросов
request_queue = asyncio.Queue()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start."""
    user_id = update.effective_user.id
    logger.info(f"Пользователь {user_id} запустил команду /start")
    try:
        await update.message.reply_text(
            "Уважаемый пользователь,\n\n"
            "Я бот для поиска информации по ИНН на сайтах ЕФРСБ и Кад.арбитр. "
            "Введите ИНН (10 или 12 цифр) для поиска."
        )
    except TimedOut:
        logger.warning(f"Тайм-аут при выполнении команды /start для пользователя {user_id}")
        await asyncio.sleep(2)
        await update.message.reply_text(
            "Уважаемый пользователь,\n\n"
            "Я бот для поиска информации по ИНН на сайтах ЕФРСБ и Кад.арбитр. "
            "Введите ИНН (10 или 12 цифр) для поиска."
        )


async def fetch_service_data(session: aiohttp.ClientSession, url: str, inn: str):
    """Отправка POST-запроса к сервису."""
    payload = {"inn": inn}
    try:
        async with session.post(url, json=payload, timeout=10) as response:
            if response.status != 200:
                logger.error(f"Ошибка HTTP {response.status} при запросе к {url} для ИНН {inn}")
                return {"error": f"HTTP {response.status}"}
            return await response.json()
    except aiohttp.ClientError as e:
        logger.error(f"Ошибка при запросе к {url} для ИНН {inn}: {str(e)}")
        return {"error": f"Ошибка сети: {str(e)}"}
    except asyncio.TimeoutError:
        logger.error(f"Тайм-аут при запросе к {url} для ИНН {inn}")
        return {"error": "Тайм-аут запроса"}


async def process_request(inn: str, update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка запроса из очереди."""
    user_id = update.effective_user.id
    start_time = time.time()
    logger.info(f"Начало обработки запроса для ИНН {inn} от пользователя {user_id}")

    waiting_message = await update.message.reply_text("Обработка данных начата. Пожалуйста, ожидайте.")

    try:
        async with aiohttp.ClientSession() as session:
            # Параллельные запросы к сервисам
            efrsb_task = fetch_service_data(session, EFRSB_URL, inn)
            kad_arbitr_task = fetch_service_data(session, KAD_ARBITR_URL, inn)
            efrsb_data, kad_arbitr_data = await asyncio.gather(efrsb_task, kad_arbitr_task, return_exceptions=True)

        # Формирование отчета
        report = [f"Отчет по должнику (ИНН: {inn})", "============================="]
        report.append("\n1. Основные данные")
        report.append("-------------------")
        report.append(f"- ИНН: {inn}")

        # 2. ЕФРСБ
        report.append("\n2. ЕФРСБ")
        report.append("-------------------")
        if isinstance(efrsb_data, dict) and efrsb_data.get("status") == "success":
            individuals = efrsb_data.get("individuals", [])
            legal_entities = efrsb_data.get("legal_entities", [])
            if not (individuals or legal_entities):
                report.append("- Банкротство: Не найдено")
            else:
                report.append("- Банкротство:")
                if individuals:
                    for idx, person in enumerate(individuals, 1):
                        report.append(f"  - Физическое лицо {idx}:")
                        report.append(f"    - ФИО: {person.get('full_name', 'Неизвестно')}")
                        report.append(f"    - Адрес: {person.get('address', 'Неизвестно')}")
                        report.append(f"    - Статус: {person.get('status', 'Неизвестно')}")
                        report.append(f"    - Дата статуса: {person.get('status_date', 'Неизвестно')}")
                        report.append(f"    - Номер дела: {person.get('court_case_number', 'Неизвестно')}")
                        report.append(
                            f"    - Арбитражный управляющий: {person.get('arbitration_manager', 'Неизвестно')}")
                if legal_entities:
                    for idx, entity in enumerate(legal_entities, 1):
                        report.append(f"  - Юридическое лицо {idx}:")
                        report.append(f"    - Название: {entity.get('name', 'Неизвестно')}")
                        report.append(f"    - ИНН: {entity.get('inn', 'Неизвестно')}")
                        report.append(f"    - Статус: {entity.get('status', 'Неизвестно')}")
                        report.append(f"    - Дата статуса: {entity.get('status_date', 'Неизвестно')}")
                        report.append(f"    - Номер дела: {entity.get('court_case_number', 'Неизвестно')}")
                        report.append(
                            f"    - Арбитражный управляющий: {entity.get('arbitration_manager', 'Неизвестно')}")
        else:
            error_msg = efrsb_data.get("error", "Неизвестная ошибка") if isinstance(efrsb_data,
                                                                                    dict) else "Некорректные данные"
            report.append(f"- Статус: Ошибка: {error_msg}")
            logger.error(f"Ошибка в данных ЕФРСБ для ИНН {inn}: {error_msg}")

        # 3. Кад.арбитр
        report.append("\n3. Кад.арбитр")
        report.append("-------------------")
        if isinstance(kad_arbitr_data, dict) and kad_arbitr_data.get("status") == "success":
            cases = kad_arbitr_data.get("data", {}).get("cases", [])
            if not cases:
                report.append("- Судебные дела: Не найдены")
            else:
                report.append("- Судебные дела:")
                for idx, case in enumerate(cases, 1):
                    report.append(f"  - Дело {idx}:")
                    report.append(f"    - Номер дела: {case.get('case_number', 'Неизвестно')}")
                    report.append(f"    - Дата регистрации: {case.get('registration_date', 'Неизвестно')}")
                    report.append(f"    - Судья: {case.get('judge', 'Неизвестно')}")
                    report.append(f"    - Текущая инстанция: {case.get('current_instance', 'Неизвестно')}")
                    report.append(f"    - Истец: {case.get('plaintiff', 'Неизвестно')}")
                    report.append(f"    - Ответчик: {case.get('respondent', 'Неизвестно')}")
        else:
            error_msg = kad_arbitr_data.get("error", "Неизвестная ошибка") if isinstance(kad_arbitr_data,
                                                                                         dict) else "Некорректные данные"
            report.append(f"- Статус: Ошибка: {error_msg}")
            logger.error(f"Ошибка в данных Кад.арбитр для ИНН {inn}: {error_msg}")

        report.append("=============================")
        response = "\n".join(report)

        # Удаление сообщения об ожидании
        try:
            await waiting_message.delete()
        except Exception as e:
            logger.warning(f"Ошибка при удалении сообщения об ожидании для ИНН {inn}: {str(e)}")

        # Отправка результата
        for attempt in range(3):
            try:
                await update.message.reply_text(response)
                logger.info(f"Запрос для ИНН {inn} успешно обработан за {time.time() - start_time:.2f} секунд")
                return
            except TimedOut:
                logger.warning(f"Тайм-аут при отправке результата для ИНН {inn} (попытка {attempt + 1}/3)")
                await asyncio.sleep(2)
        logger.error(f"Не удалось отправить результат для ИНН {inn} после 3 попыток")
        await update.message.reply_text("Ошибка связи с сервером. Пожалуйста, попробуйте снова.")

    except Exception as e:
        logger.error(f"Критическая ошибка при обработке ИНН {inn}: {str(e)}", exc_info=True)
        try:
            await waiting_message.delete()
        except Exception as e:
            logger.warning(f"Ошибка при удалении сообщения об ожидании для ИНН {inn}: {str(e)}")
        try:
            await update.message.reply_text(f"Произошла ошибка: {str(e)}. Пожалуйста, попробуйте снова.")
        except TimedOut:
            logger.warning(f"Тайм-аут при отправке сообщения об ошибке для ИНН {inn}")
            await asyncio.sleep(2)
            await update.message.reply_text(f"Произошла ошибка: {str(e)}. Пожалуйста, попробуйте снова.")


async def worker(context: ContextTypes.DEFAULT_TYPE):
    """Фоновая задача для обработки очереди запросов."""
    while True:
        try:
            # Получаем задачу из очереди
            inn, update = await request_queue.get()
            logger.info(f"Воркер взял запрос для ИНН {inn} от пользователя {update.effective_user.id}")
            await process_request(inn, update, context)
            request_queue.task_done()
        except Exception as e:
            logger.error(f"Ошибка в воркере: {str(e)}", exc_info=True)
            await asyncio.sleep(2)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений."""
    user_id = update.effective_user.id
    inn = update.message.text.strip()
    logger.info(f"Получено сообщение от пользователя {user_id}: ИНН {inn}")

    # Валидация ИНН
    if not re.match(r'^\d{10}$|^\d{12}$', inn):
        logger.warning(f"Некорректный ИНН {inn} от пользователя {user_id}")
        try:
            await update.message.reply_text("Ошибка: ИНН должен содержать 10 или 12 цифр.")
            return
        except TimedOut:
            logger.warning(f"Тайм-аут при отправке ошибки валидации ИНН {inn} для пользователя {user_id}")
            await asyncio.sleep(2)
            await update.message.reply_text("Ошибка: ИНН должен содержать 10 или 12 цифр.")
            return

    # Ограничение размера очереди
    if request_queue.qsize() >= 10:
        logger.warning(f"Очередь переполнена для ИНН {inn} от пользователя {user_id}")
        try:
            await update.message.reply_text("Очередь переполнена. Пожалуйста, попробуйте позже.")
            return
        except TimedOut:
            logger.warning(f"Тайм-аут при отправке сообщения о переполнении очереди для ИНН {inn}")
            await asyncio.sleep(2)
            await update.message.reply_text("Очередь переполнена. Пожалуйста, попробуйте позже.")
            return

    # Добавление запроса в очередь
    await request_queue.put((inn, update))
    queue_size = request_queue.qsize()
    logger.info(f"Запрос для ИНН {inn} добавлен в очередь. Размер очереди: {queue_size}")
    try:
        await update.message.reply_text(
            f"Ваш запрос принят. В очереди {queue_size} запрос(ов). Пожалуйста, ожидайте."
        )
    except TimedOut:
        logger.warning(f"Тайм-аут при отправке уведомления о постановке в очередь для ИНН {inn}")
        await asyncio.sleep(2)
        await update.message.reply_text(
            f"Ваш запрос принят. В очереди {queue_size} запрос(ов). Пожалуйста, ожидайте."
        )


def main():
    """Запуск Telegram-бота."""
    try:
        application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
        application.add_handler(CommandHandler("start", start))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

        # Запуск воркера
        loop = asyncio.get_event_loop()
        loop.create_task(worker(application.context_types.context))

        logger.info("Запуск бота...")
        print("Бот запущен")
        application.run_polling()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Остановка бота...")
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {str(e)}", exc_info=True)
        print(f"Ошибка при запуске бота: {str(e)}")
        exit(1)


if __name__ == '__main__':
    main()
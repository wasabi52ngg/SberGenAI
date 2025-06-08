import asyncio
import logging
import json
import re
import psutil
import time
from flask import Flask, request, jsonify
from playwright.async_api import async_playwright, Error as PlaywrightError
from bs4 import BeautifulSoup

# Настройка минимального логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Подавление HTTP-логов
logging.getLogger('httpx').setLevel(logging.WARNING)

app = Flask(__name__)

# Ограничения
MAX_CONCURRENT_REQUESTS = 2  # Параллельные запросы на сервис
GLOBAL_SEMAPHORE = asyncio.Semaphore(10)  # Общий лимит страниц

def log_memory_usage():
    """Логирование потребления памяти."""
    process = psutil.Process()
    mem_info = process.memory_info()
    logger.info(f"Потребление памяти: {mem_info.rss / 1024 / 1024:.2f} МБ")

def parse_date(date_str: str) -> tuple:
    """Парсинг даты из формата dd.mm.yyyy"""
    if not date_str:
        return "default", "default", ""
    try:
        day, month, year = date_str.split('.')
        return day.zfill(2), month.zfill(2), year
    except:
        return "default", "default", ""

async def get_probate_case(name: str, birth_date: str, semaphore: asyncio.Semaphore, cdp_endpoint: str = "http://localhost:9222") -> str:
    """Получение данных о наследственных делах с notariat.ru"""
    url = "https://notariat.ru/ru-ru/help/probate-cases/"

    async with GLOBAL_SEMAPHORE:
        async with async_playwright() as p:
            try:
                logger.info(f"Подключение к CDP: {cdp_endpoint}")
                browser = await p.chromium.connect_over_cdp(cdp_endpoint)
                page = await browser.contexts[0].new_page()

                try:
                    logger.info("Загружаем страницу notariat.ru")
                    await page.goto(url, wait_until="domcontentloaded", timeout=10000)
                    logger.info("Ожидаем форму ввода данных (до 10 секунд)")
                    await page.wait_for_selector("input[name='name']", timeout=10000)
                    logger.info("Форма ввода данных загружена")

                    # Проверка на капчу
                    if await page.query_selector("div.captcha"):
                        logger.error("Обнаружена капча")
                        return json.dumps({"status": "error", "message": "Обнаружена капча, попробуйте позже"}, ensure_ascii=False, indent=2)

                    # Заполнение формы
                    logger.info("Заполняем форму")
                    await page.fill("input[name='name']", name)

                    # Дата рождения
                    b_day, b_month, b_year = parse_date(birth_date)
                    await page.select_option("select[name='b-day']", value=b_day)
                    await page.select_option("select[name='b-month']", value=b_month)
                    await page.fill("input[name='b-year']", b_year)

                    # Нажатие кнопки поиска
                    logger.info("Нажимаем кнопку поиска")
                    await page.click("button.js-probate-cases__submit")
                    logger.info("Ожидаем результаты (до 15 секунд)")
                    await page.wait_for_selector("div.probate-cases__plate_result, h5.probate-cases__result-header", timeout=15000)

                    # Парсинг результатов в памяти
                    content = await page.content()
                    soup = BeautifulSoup(content, 'html.parser')

                    result = {"status": "success", "case": {}}

                    # Проверяем оба возможных варианта результатов
                    result_block = soup.find("div", class_="probate-cases__plate_result") or \
                                  soup.find("div", class_="probate-cases__result")

                    if not result_block:
                        logger.error("Не удалось распознать структуру результатов")
                        return json.dumps({"status": "error", "message": "Не удалось распознать структуру результатов"},
                                        ensure_ascii=False, indent=2)

                    # Обрабатываем случай с нулевыми результатами
                    zero_results = result_block.find("h5", class_="probate-cases__result-header")
                    if zero_results and "0" in zero_results.get_text():
                        logger.info("Наследственных дел не найдено")
                        return json.dumps({
                            "status": "success",
                            "result": "Наследственных дел не найдено",
                            "details": zero_results.get_text(strip=True)
                        }, ensure_ascii=False, indent=2)

                    # Обрабатываем случай с найденными данными
                    try:
                        result['case']['name'] = result_block.find("b", class_="js-rp__name").get_text(strip=True)
                    except AttributeError:
                        result['case']['name'] = "Не указано"

                    try:
                        result['case']['birth_date'] = result_block.find("b", class_="js-rp__date-birth").get_text(strip=True)
                    except AttributeError:
                        result['case']['birth_date'] = "Не указана"

                    # Обрабатываем записи наследственных дел
                    records = result_block.find("b", class_='probate-cases__records')
                    result['case']['probate-cases__records'] = records.get_text(strip=True) if records else "Записей не найдено"
                    logger.info("Данные с сайта notariat получены")
                    log_memory_usage()
                    return json.dumps(result, ensure_ascii=False, indent=2)

                except PlaywrightError as e:
                    logger.error(f"Ошибка Playwright: {str(e)}")
                    return json.dumps({"status": "error", "message": str(e)}, ensure_ascii=False, indent=2)

                finally:
                    await page.close()
                    await browser.close()

            except PlaywrightError as e:
                logger.error(f"Ошибка подключения: {str(e)}")
                return json.dumps({"status": "error", "message": str(e)}, ensure_ascii=False, indent=2)

async def process_multiple_cases(cases: list, cdp_endpoint: str) -> list:
    """Параллельная обработка списка дел."""
    start_time = time.time()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    tasks = [get_probate_case(case["name"], case["birth_date"], semaphore, cdp_endpoint) for case in cases]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    logger.info(f"Обработка {len(cases)} дел заняла {time.time() - start_time:.2f} секунд")
    log_memory_usage()
    return results

@app.route('/probate', methods=['POST'])
async def probate_endpoint():
    """Эндпоинт для получения данных о наследственных делах с notariat.ru."""
    data = request.get_json()
    name = data.get('name')
    birth_date = data.get('birth_date')
    cases = data.get('cases', [])
    cdp_endpoint = data.get('cdp_endpoint', 'http://localhost:9222')

    def is_valid_name(name):
        return name and re.match(r'^[\w\sа-яА-ЯёЁ-]+$', name, re.UNICODE)

    def is_valid_birth_date(birth_date):
        return birth_date and re.match(r'^\d{2}\.\d{2}\.\d{4}$', birth_date)

    if name and birth_date and is_valid_name(name) and is_valid_birth_date(birth_date):
        try:
            result = await get_probate_case(name, birth_date, asyncio.Semaphore(1), cdp_endpoint)
            return jsonify(json.loads(result))
        except Exception as e:
            logger.error(f"Ошибка обработки запроса для имени {name}: {str(e)}")
            return jsonify({"error": str(e)}), 500
    elif cases:
        invalid_cases = [
            case for case in cases
            if not (case.get('name') and case.get('birth_date') and
                    is_valid_name(case['name']) and is_valid_birth_date(case['birth_date']))
        ]
        if invalid_cases:
            return jsonify({"error": f"Неверный формат данных: {invalid_cases}"}), 400
        try:
            results = await process_multiple_cases(cases, cdp_endpoint)
            parsed_results = [json.loads(result) if isinstance(result, str) else {"error": str(result)} for result in results]
            return jsonify({"results": parsed_results})
        except Exception as e:
            logger.error(f"Ошибка обработки списка дел: {str(e)}")
            return jsonify({"error": f"Ошибка обработки списка дел: {str(e)}"}), 500
    else:
        return jsonify({"error": "Не указаны имя и дата рождения или список дел"}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5005)
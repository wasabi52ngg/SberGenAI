import asyncio
import logging
import re
import psutil
import time
from flask import Flask, request, jsonify
from playwright.async_api import async_playwright, Error as PlaywrightError

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)
logging.getLogger('httpx').setLevel(logging.WARNING)

app = Flask(__name__)

# Ограничения
MAX_CONCURRENT_REQUESTS = 2  # Параллельные запросы на сервис
GLOBAL_SEMAPHORE = asyncio.Semaphore(10)  # Общий лимит страниц для всех сервисов

def log_memory_usage():
    """Логирование потребления памяти."""
    process = psutil.Process()
    mem_info = process.memory_info()
    logger.info(f"Потребление памяти: {mem_info.rss / 1024 / 1024:.2f} МБ")

async def get_pledge_info(vin: str, semaphore: asyncio.Semaphore, cdp_endpoint: str = "http://localhost:9222") -> dict:
    """Получение данных о залоге ТС с reestr-zalogov.ru для одного VIN."""
    async with GLOBAL_SEMAPHORE:
        async with async_playwright() as p:
            try:
                logger.info(f"Подключение к CDP по адресу: {cdp_endpoint} для VIN {vin}")
                browser = await p.chromium.connect_over_cdp(cdp_endpoint)
                context = await browser.new_context()
                start_time = time.time()
                page = await context.new_page()
            except PlaywrightError as e:
                logger.error(f"Ошибка инициализации браузера для VIN {vin}: {str(e)}")
                return {"status": "error", "message": f"Ошибка инициализации браузера: {str(e)}", "vin": vin}

            max_attempts = 3
            last_error = None

            for attempt in range(1, max_attempts + 1):
                try:
                    url = "https://www.reestr-zalogov.ru/search/index"
                    logger.info(f"Попытка {attempt} из {max_attempts}: Загружаю страницу reestr-zalogov.ru для VIN {vin}")
                    await page.goto(url, wait_until="networkidle", timeout=60000)

                    # Проверка на капчу
                    if await page.query_selector("div.captcha"):
                        logger.error(f"Обнаружена капча для VIN {vin}")
                        return {"status": "error", "message": "Обнаружена капча, попробуйте позже", "vin": vin}

                    # Выбор категории поиска - "По информации о предмете залога"
                    logger.info(f"Выбираю категорию поиска для VIN {vin}")
                    await page.click("ul.nav-pills > li:nth-child(3) > a")
                    await page.wait_for_timeout(2000)

                    # Выбор типа ТС - "Транспортное средство"
                    logger.info(f"Выбираю транспортное средство для VIN {vin}")
                    await page.click("div[data-v-610150a0] ul.nav-pills > li:nth-child(1) > a")
                    await page.wait_for_timeout(2000)

                    # Ожидание формы ввода VIN
                    logger.info(f"Ожидаю форму ввода для VIN {vin}")
                    await page.wait_for_selector("input#vehicleProperty\\.vin", timeout=10000)

                    # Заполнение VIN
                    logger.info(f"Ввожу VIN {vin}")
                    vin_input = await page.query_selector("input#vehicleProperty\\.vin")
                    if not vin_input:
                        logger.error(f"Поле VIN не найдено для VIN {vin}")
                        return {"status": "error", "message": "Поле VIN не найдено", "vin": vin}
                    await vin_input.fill(vin)
                    await page.wait_for_timeout(1000)

                    # Нажатие кнопки поиска
                    logger.info(f"Нажимаю кнопку поиска для VIN {vin}")
                    await page.click("#find-btn")
                    await page.wait_for_selector("div.search-results, div.search-error-label", timeout=15000)

                    # Извлечение данных с помощью JavaScript
                    result = {"status": "success", "results": {"details": []}, "vin": vin}
                    data = await page.evaluate("""
                        () => {
                            const result = { details: [], search_params: '' };
                            const errorBlock = document.querySelector('div.search-error-label');
                            if (errorBlock && errorBlock.textContent.includes('не найдено')) {
                                return { error: 'По заданным критериям поиска сведений не найдено' };
                            }

                            const searchParams = document.querySelector('div.search-params-tip');
                            if (searchParams) {
                                result.search_params = searchParams.textContent.trim();
                            }

                            const table = document.querySelector('table.search-results');
                            if (table) {
                                const rows = table.querySelectorAll('tr');
                                rows.forEach((row, index) => {
                                    if (index === 0) return; // Пропускаем заголовок
                                    const cols = row.querySelectorAll('td');
                                    if (cols.length >= 3) {
                                        result.details.push({
                                            case_number: cols[0].textContent.trim(),
                                            date: cols[1].textContent.trim(),
                                            pledgor: cols[2].textContent.trim()
                                        });
                                    }
                                });
                            }
                            return result;
                        }
                    """)

                    if "error" in data:
                        logger.info(f"Данные для VIN {vin}: Ничего не найдено")
                        return {"status": "success", "results": {}, "vin": vin}

                    result["results"]["search_params"] = data["search_params"]
                    result["results"]["details"] = data["details"]

                    logger.info(f"Найдено записей для VIN {vin}: {len(data['details'])}")
                    logger.info(f"Обработка VIN {vin} заняла {time.time() - start_time:.2f} секунд")
                    log_memory_usage()
                    return result

                except PlaywrightError as e:
                    logger.error(f"Попытка {attempt} не удалась для VIN {vin}: {str(e)}")
                    last_error = str(e)
                    if attempt < max_attempts:
                        logger.info(f"Ожидаю 3 секунды перед повторной попыткой для VIN {vin}")
                        await page.wait_for_timeout(3000)
                        continue
                    return {"status": "error", "message": f"Ошибка после {max_attempts} попыток: {last_error}", "vin": vin}

                finally:
                    try:
                        await page.close()
                        await context.close()
                        await browser.close()
                    except Exception as e:
                        logger.error(f"Ошибка при закрытии ресурсов для VIN {vin}: {str(e)}")

async def process_multiple_vins(vins: list, cdp_endpoint: str) -> list:
    """Параллельная обработка списка VIN."""
    start_time = time.time()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    tasks = [get_pledge_info(vin, semaphore, cdp_endpoint) for vin in vins]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    logger.info(f"Обработка {len(vins)} VIN заняла {time.time() - start_time:.2f} секунд")
    log_memory_usage()
    return results

@app.route('/pledge', methods=['POST'])
async def pledge_endpoint():
    """Эндпоинт для получения данных о залоге с reestr-zalogov.ru для одного или нескольких VIN."""
    data = request.get_json()
    vin = data.get('vin')
    vins = data.get('vins', [])
    cdp_endpoint = data.get('cdp_endpoint', 'http://localhost:9222')

    def is_valid_vin(vin):
        return vin and re.match(r'^[A-HJ-NPR-Z0-9]{17}$', vin, re.IGNORECASE)

    if vin and is_valid_vin(vin):
        try:
            result = await get_pledge_info(vin, asyncio.Semaphore(1), cdp_endpoint)
            return jsonify(result)
        except Exception as e:
            logger.error(f"Ошибка обработки запроса для VIN {vin}: {str(e)}")
            return jsonify({"status": "error", "message": f"Ошибка обработки запроса: {str(e)}"}), 500
    elif vins:
        invalid_vins = [vin for vin in vins if not is_valid_vin(vin)]
        if invalid_vins:
            return jsonify({"status": "error", "message": f"Неверный формат VIN: {invalid_vins}"}), 400
        try:
            results = await process_multiple_vins(vins, cdp_endpoint)
            return jsonify({"status": "success", "results": results})
        except Exception as e:
            logger.error(f"Ошибка обработки списка VIN: {str(e)}")
            return jsonify({"status": "error", "message": f"Ошибка обработки списка VIN: {str(e)}"}), 500
    else:
        return jsonify({"status": "error", "message": "Не указан VIN или список VIN"}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5008)
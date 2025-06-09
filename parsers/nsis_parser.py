import asyncio
import logging
import re
import psutil
import time
from flask import Flask, request, jsonify
from playwright.async_api import async_playwright, Error as PlaywrightError
import os
import dotenv

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

# Загрузка переменных окружения
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
dotenv.load_dotenv(dotenv_path)

proxy_pool = [
    {
        "server": os.getenv("PROXY_SERVER"),
        "username": os.getenv("PROXY_USERNAME"),
        "password": os.getenv("PROXY_PASSWORD")
    },
]


def log_memory_usage():
    """Логирование потребления памяти."""
    process = psutil.Process()
    mem_info = process.memory_info()
    logger.info(f"Потребление памяти: {mem_info.rss / 1024 / 1024:.2f} МБ")


async def attempt_osago_check(page, vin: str) -> tuple:
    """Выполняет попытку проверки ОСАГО и возвращает результат."""
    url = "https://nsis.ru/products/osago/check/"
    try:
        logger.info(f"Загружаю страницу nsis.ru для VIN {vin}")
        await page.goto(url, wait_until="load", timeout=45000)

        # Проверка на превышение лимита запросов
        info_block = await page.query_selector("div.infoBlock")
        if info_block and "Вы превысили количество запросов" in await info_block.inner_text():
            return {"status": "error", "message": "Превышен лимит запросов в час", "vin": vin}, False

        await page.wait_for_selector("input[name='vin']", timeout=10000)

        # Проверка активной вкладки
        active_tab = await page.query_selector("ul.tabs__nav li.tabs__navItem button.btn--isActive")
        if not active_tab or "По транспортному средству (ТС)" not in await active_tab.inner_text():
            logger.info(f"Переключаюсь на вкладку 'По транспортному средству (ТС)' для VIN {vin}")
            await page.click("ul.tabs__nav li.tabs__navItem button:has-text('По транспортному средству (ТС)')")

        logger.info(f"Ввожу VIN {vin}")
        await page.fill("input[name='vin']", vin)
        await page.click("button[type='submit']")

        # Ожидание результата, ошибки или отсутствия данных
        await page.wait_for_selector(
            "div.policyDataModal, div#modal-policy-not-found, div#modal-error",
            timeout=15000
        )

        # Проверка на окно отсутствия данных
        not_found_modal = await page.query_selector("div#modal-policy-not-found")
        if not_found_modal:
            logger.info(f"Сведения об ОСАГО для VIN {vin} не найдены")
            return {"status": "success", "results": {}, "vin": vin}, False

        # Проверка на окно ошибки
        error_modal = await page.query_selector("div#modal-error")
        if error_modal:
            logger.warning(f"Обнаружено окно ошибки для VIN {vin}")
            return {"status": "error", "message": "Ошибка на сайте", "vin": vin}, True

        # Данные найдены, извлекаем их
        policy_data = await page.evaluate("""
            () => {
                const policyModal = document.querySelector('div.policyDataModal');
                if (!policyModal) return null;

                const policy = {};
                const dateSlot = policyModal.querySelector('div.policyDataModal__dateSlot');
                if (dateSlot) {
                    policy.check_date = dateSlot.textContent.trim();
                }

                const dataLists = policyModal.querySelectorAll('dl.dataList__list');
                dataLists.forEach(dataList => {
                    const items = dataList.querySelectorAll('div.dataList__item');
                    items.forEach(item => {
                        const label = item.querySelector('dt')?.textContent.trim().replace(':', '').toLowerCase().replace(/\\s+/g, '_');
                        const value = item.querySelector('dd')?.textContent.trim();
                        if (label && value) {
                            policy[label] = value;
                        }
                    });
                });
                return policy;
            }
        """)

        if not policy_data:
            logger.warning(f"Не удалось извлечь данные для VIN {vin}")
            return {"status": "error", "message": "Не удалось извлечь данные из модального окна", "vin": vin}, True

        logger.info(f"Найдены данные ОСАГО для VIN {vin}")
        return {"status": "success", "policies": [policy_data], "vin": vin}, False

    except PlaywrightError as e:
        logger.error(f"Ошибка Playwright для VIN {vin}: {str(e)}")
        return {"status": "error", "message": f"Ошибка загрузки страницы: {str(e)}", "vin": vin}, True


async def get_info_osago(vin: str, semaphore: asyncio.Semaphore, cdp_endpoint: str = "http://localhost:9222") -> dict:
    """Основная функция получения данных ОСАГО для одного VIN."""
    async with GLOBAL_SEMAPHORE:
        async with async_playwright() as p:
            try:
                logger.info(f"Подключение к CDP по адресу: {cdp_endpoint} для VIN {vin}")
                browser = await p.chromium.connect_over_cdp(cdp_endpoint)
                context = await browser.new_context(proxy=proxy_pool[0])
                start_time = time.time()
                page = await context.new_page()
            except PlaywrightError as e:
                logger.error(f"Ошибка инициализации браузера для VIN {vin}: {str(e)}")
                return {"status": "error", "message": f"Ошибка инициализации браузера: {str(e)}", "vin": vin}

            try:
                max_attempts = 3
                last_error = None

                for attempt in range(1, max_attempts + 1):
                    try:
                        result, can_retry = await attempt_osago_check(page, vin)
                        if not can_retry:
                            logger.info(f"Обработка VIN {vin} завершена за {time.time() - start_time:.2f} секунд")
                            log_memory_usage()
                            return result

                        logger.info(f"Попытка {attempt} для VIN {vin} не удалась, повторная попытка через 3 секунды")
                        await page.wait_for_timeout(3000)

                    except Exception as e:
                        last_error = str(e)
                        if attempt < max_attempts:
                            logger.info(f"Попытка {attempt} для VIN {vin} не удалась: {str(e)}, повторная попытка")
                            await page.wait_for_timeout(3000)
                            continue
                        return {"status": "error", "message": f"Ошибка после {max_attempts} попыток: {last_error}",
                                "vin": vin}

                return {"status": "error", "message": f"Неизвестная ошибка после {max_attempts} попыток", "vin": vin}

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
    tasks = [get_info_osago(vin, semaphore, cdp_endpoint) for vin in vins]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    logger.info(f"Обработка {len(vins)} VIN заняла {time.time() - start_time:.2f} секунд")
    log_memory_usage()
    return results


@app.route('/osago', methods=['POST'])
async def osago_handler():
    """Обработчик запроса проверки ОСАГО для одного или нескольких VIN."""
    data = request.get_json()
    vin = data.get('vin')
    vins = data.get('vins', [])
    cdp_endpoint = data.get('cdp_endpoint', 'http://localhost:9222')

    def is_valid_vin(vin):
        # Простая проверка: VIN должен быть строкой из 17 символов (буквы и цифры)
        return vin and re.match(r'^[A-HJ-NPR-Z0-9]{17}$', vin, re.IGNORECASE)

    if vin and is_valid_vin(vin):
        try:
            result = await get_info_osago(vin, asyncio.Semaphore(1), cdp_endpoint)
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
    app.run(host='0.0.0.0', port=5006, use_reloader=False)

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

async def get_info_kad_arbitr(inn: str, semaphore: asyncio.Semaphore, cdp_endpoint: str = "http://localhost:9222") -> dict:
    """Получение данных с kad.arbitr.ru для одного ИНН."""
    async with GLOBAL_SEMAPHORE:  # Ограничение общего количества страниц
        async with async_playwright() as playwright:
            try:
                logger.info(f"Подключение к CDP по адресу: {cdp_endpoint} для ИНН {inn}")
                browser = await playwright.chromium.connect_over_cdp(cdp_endpoint)
                context = await browser.new_context()
                start_time = time.time()
                page = await context.new_page()
            except PlaywrightError as e:
                logger.error(f"Ошибка инициализации браузера для ИНН {inn}: {str(e)}")
                return {"status": "error", "message": f"Ошибка инициализации браузера: {str(e)}", "inn": inn}

            try:
                logger.info(f"Загружаю страницу kad.arbitr.ru для ИНН {inn}")
                response = await page.goto("https://kad.arbitr.ru/", wait_until="domcontentloaded", timeout=10000)

                # Проверка на пустую страницу
                if not response or response.url == "about:blank":
                    logger.error(f"Пустая страница (about:blank) для ИНН {inn}")
                    return {"status": "error", "message": "Пустая страница, возможно, сбой браузера", "inn": inn}

                content = await page.content()
                if not content or "<html" not in content.lower():
                    logger.error(f"Пустое содержимое страницы для ИНН {inn}")
                    return {"status": "error", "message": "Пустое содержимое страницы", "inn": inn}

                logger.info(f"Ожидаю поле ввода 'Участник дела' для ИНН {inn}")
                await page.wait_for_selector("div#sug-participants textarea", timeout=10000)

                notification = await page.query_selector("div.b-promo_notification")
                if notification:
                    logger.info(f"Обнаружено всплывающее уведомление для ИНН {inn}, пытаюсь закрыть")
                    close_button = await page.query_selector("a.b-promo_notification-popup-close")
                    if close_button:
                        await close_button.click()
                        await page.wait_for_timeout(500)
                        logger.info(f"Уведомление закрыто для ИНН {inn}")
                    else:
                        logger.warning(f"Кнопка закрытия уведомления не найдена для ИНН {inn}")


                logger.info(f"Ввожу ИНН {inn} в поле 'Участник дела'")
                await page.fill("div#sug-participants textarea", inn)
                await page.click("div#b-form-submit button")

                logger.info(f"Ожидаю результаты поиска для ИНН {inn}")
                await page.wait_for_selector(
                    "div.b-results, div.b-noResults:not(.g-hidden), div.b-case-loading",
                    timeout=10000
                )

                async def is_loading_complete():
                    loading = await page.query_selector("div.b-case-loading:not([style*='display: none'])")
                    return loading is None

                for _ in range(10):
                    if await is_loading_complete():
                        break
                    await page.wait_for_timeout(500)

                if not await is_loading_complete():
                    logger.warning(f"Загрузка результатов не завершена вовремя для ИНН {inn}")
                    return {"status": "error", "message": "Загрузка результатов не завершена", "inn": inn}

                no_results = await page.query_selector("div.b-noResults:not(.g-hidden)")
                if no_results:
                    logger.info(f"Данные для ИНН {inn}: Ничего не найдено")
                    return {"status": "success", "data": {"cases": []}, "inn": inn}

                result = {"status": "success", "data": {"cases": []}, "inn": inn}
                cases = await page.evaluate("""
                    () => {
                        const rows = document.querySelectorAll('table#b-cases tbody tr');
                        const cases = [];
                        rows.forEach(row => {
                            const caseData = {};
                            const numCase = row.querySelector('a.num_case');
                            caseData.case_number = numCase ? numCase.textContent.trim() : '';
                            const date = row.querySelector('div.bankruptcy span');
                            caseData.registration_date = date ? date.textContent.trim() : '';
                            const courtCell = row.querySelector('td.court');
                            if (courtCell) {
                                const judge = courtCell.querySelector('div.judge');
                                caseData.judge = judge ? judge.textContent.trim() : '';
                                const instance = courtCell.querySelectorAll('div')[courtCell.querySelectorAll('div').length - 1];
                                caseData.current_instance = instance ? instance.textContent.trim() : '';
                            }
                            const plaintiff = row.querySelector('td.plaintiff span.js-rollover');
                            caseData.plaintiff = plaintiff ? plaintiff.textContent.trim() : '';
                            const respondent = row.querySelector('td.respondent span.js-rollover');
                            caseData.respondent = respondent ? respondent.textContent.trim() : '';
                            const rollover = row.querySelector('span.js-rolloverHtml');
                            if (rollover) {
                                const innSpan = rollover.querySelector('span.g-highlight');
                                caseData.inn = innSpan ? innSpan.textContent.trim() : '';
                            }
                            cases.push(caseData);
                        });
                        return cases;
                    }
                """)

                result['data']['cases'] = cases
                logger.info(f"Найдено строк в таблице для ИНН {inn}: {len(cases)}")
                logger.info(f"Обработка ИНН {inn} заняла {time.time() - start_time:.2f} секунд")
                log_memory_usage()
                return result

            except PlaywrightError as e:
                logger.error(f"Ошибка при загрузке страницы или взаимодействии для ИНН {inn}: {str(e)}")
                return {"status": "error", "message": f"Ошибка загрузки страницы или взаимодействия: {str(e)}", "inn": inn}
            finally:
                try:
                    await page.close()
                    await context.close()
                    await browser.close()
                except Exception as e:
                    logger.error(f"Ошибка при закрытии ресурсов для ИНН {inn}: {str(e)}")

async def process_multiple_inns(inns: list, cdp_endpoint: str) -> list:
    """Параллельная обработка списка ИНН."""
    start_time = time.time()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    tasks = [get_info_kad_arbitr(inn, semaphore, cdp_endpoint) for inn in inns]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    logger.info(f"Обработка {len(inns)} ИНН заняла {time.time() - start_time:.2f} секунд")
    log_memory_usage()
    return results

@app.route('/kad_arbitr', methods=['POST'])
async def kad_arbitr_endpoint():
    """Эндпоинт для получения данных Кад.арбитр для одного или нескольких ИНН."""
    data = request.get_json()
    inn = data.get('inn')
    inns = data.get('inns', [])
    cdp_endpoint = data.get('cdp_endpoint', 'http://localhost:9222')

    def is_valid_inn(inn):
        return inn and re.match(r'^\d{10}$|^\d{12}$', inn)

    if inn and is_valid_inn(inn):
        try:
            result = await get_info_kad_arbitr(inn, asyncio.Semaphore(1), cdp_endpoint)
            return jsonify(result)
        except Exception as e:
            logger.error(f"Ошибка обработки запроса для ИНН {inn}: {str(e)}")
            return jsonify({"status": "error", "message": f"Ошибка обработки запроса: {str(e)}"}), 500
    elif inns:
        invalid_inns = [inn for inn in inns if not is_valid_inn(inn)]
        if invalid_inns:
            return jsonify({"status": "error", "message": f"Неверный формат ИНН: {invalid_inns}"}), 400
        try:
            results = await process_multiple_inns(inns, cdp_endpoint)
            return jsonify({"status": "success", "results": results})
        except Exception as e:
            logger.error(f"Ошибка обработки списка ИНН: {str(e)}")
            return jsonify({"status": "error", "message": f"Ошибка обработки списка ИНН: {str(e)}"}), 500
    else:
        return jsonify({"status": "error", "message": "Не указан ИНН или список ИНН"}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002)
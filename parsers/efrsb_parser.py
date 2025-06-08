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

async def get_info_kadrsb(inn: str, semaphore: asyncio.Semaphore, cdp_endpoint: str = "http://localhost:9222") -> dict:
    """Получение данных с ЕФРСБ для одного ИНН."""
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
                url = f"https://bankrot.fedresurs.ru/bankrupts?searchString={inn}"
                logger.info(f"Загружаю страницу ЕФРСБ для ИНН {inn}: {url}")
                response = await page.goto(url, wait_until="domcontentloaded", timeout=10000)

                # Проверка на пустую страницу
                if not response or response.url == "about:blank":
                    logger.error(f"Пустая страница (about:blank) для ИНН {inn}")
                    return {"status": "error", "message": "Пустая страница, возможно, сбой браузера", "inn": inn}

                content = await page.content()
                if not content or "<html" not in content.lower():
                    logger.error(f"Пустое содержимое страницы для ИНН {inn}")
                    return {"status": "error", "message": "Пустое содержимое страницы", "inn": inn}

                # Ожидание загрузки результатов
                logger.info(f"Ожидаю результаты поиска для ИНН {inn}")
                await page.wait_for_selector(
                    "div.u-card-result, div.no-result-msg__header, div.load-info",
                    timeout=10000
                )

                async def is_loading_complete():
                    # Проверяем наличие результатов или сообщения об отсутствии результатов
                    result_cards = await page.query_selector_all("div.u-card-result")
                    no_result = await page.query_selector("div.no-result-msg__header")
                    loading = await page.query_selector("div.load-info:not([style*='display: none'])")
                    logger.debug(f"Состояние загрузки для ИНН {inn}: cards={len(result_cards)}, no_result={no_result is not None}, loading_visible={loading is not None}")
                    return len(result_cards) > 0 or (no_result is not None and len(result_cards) == 0)

                for _ in range(20):  # Увеличиваем до 10 секунд (20 * 500ms)
                    if await is_loading_complete():
                        logger.info(f"Загрузка результатов завершена для ИНН {inn}")
                        break
                    await page.wait_for_timeout(500)

                if not await is_loading_complete():
                    logger.warning(f"Загрузка результатов не завершена вовремя для ИНН {inn}")
                    return {"status": "error", "message": "Загрузка результатов не завершена", "inn": inn}

                # Проверка на отсутствие результатов
                no_result = await page.query_selector("div.no-result-msg__header")
                if no_result and not await page.query_selector("div.u-card-result"):
                    logger.info(f"Данные ЕФРСБ для ИНН {inn}: Ничего не найдено")
                    return {"status": "success", "legal_entities": [], "individuals": [], "inn": inn}

                # Извлечение данных с помощью JavaScript
                result = {"status": "success", "legal_entities": [], "individuals": [], "inn": inn}
                cards = await page.evaluate("""
                    () => {
                        const cards = document.querySelectorAll('div.u-card-result');
                        const result = { legal_entities: [], individuals: [] };
                        cards.forEach(card => {
                            const entry = {};
                            const name = card.querySelector('div.u-card-result__name');
                            entry.name = name ? name.textContent.trim() : '';
                            const address = card.querySelector('div.u-card-result__value_adr');
                            entry.address = address ? address.textContent.trim() : '';
                            const innElem = card.querySelector('span.u-card-result__point');
                            const innValue = innElem && innElem.textContent.includes('ИНН') ? innElem.nextElementSibling : null;
                            entry.inn = innValue ? innValue.textContent.trim() : '';
                            const ogrnElem = card.querySelector('span.u-card-result__point');
                            const ogrnValue = ogrnElem && ogrnElem.textContent.includes('ОГРН') ? ogrnElem.nextElementSibling : null;
                            if (ogrnValue) {
                                entry.ogrn = ogrnValue.textContent.trim();
                                result.legal_entities.push(entry);
                            } else {
                                const snilsElem = card.querySelector('span.u-card-result__point');
                                const snilsValue = snilsElem && snilsElem.textContent.includes('СНИЛС') ? snilsElem.nextElementSibling : null;
                                entry.snils = snilsValue ? snilsValue.textContent.trim() : '';
                                result.individuals.push(entry);
                            }
                            const status = card.querySelector('div.u-card-result__value_item-property');
                            entry.status = status ? status.textContent.trim() : '';
                            const statusDate = card.querySelector('div.status-date');
                            entry.status_date = statusDate ? statusDate.textContent.trim() : '';
                            const courtCase = card.querySelector('div.u-card-result__court-case div.u-card-result__value');
                            entry.court_case_number = courtCase ? courtCase.textContent.trim() : '';
                            const manager = card.querySelector('div.u-card-result__manager div.u-card-result__value');
                            entry.arbitration_manager = manager ? manager.textContent.trim() : '';
                        });
                        return result;
                    }
                """)

                result['legal_entities'] = cards['legal_entities']
                result['individuals'] = cards['individuals']
                logger.info(f"Найдено карточек для ИНН {inn}: {len(cards['legal_entities']) + len(cards['individuals'])}")
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
    tasks = [get_info_kadrsb(inn, semaphore, cdp_endpoint) for inn in inns]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    logger.info(f"Обработка {len(inns)} ИНН заняла {time.time() - start_time:.2f} секунд")
    log_memory_usage()
    return results

@app.route('/efrsb', methods=['POST'])
async def efrsb_endpoint():
    """Эндпоинт для получения данных ЕФРСБ для одного или нескольких ИНН."""
    data = request.get_json()
    inn = data.get('inn')
    inns = data.get('inns', [])
    cdp_endpoint = data.get('cdp_endpoint', 'http://localhost:9222')

    def is_valid_inn(inn):
        return inn and re.match(r'^\d{10}$|^\d{12}$', inn)

    if inn and is_valid_inn(inn):
        try:
            result = await get_info_kadrsb(inn, asyncio.Semaphore(1), cdp_endpoint)
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
    app.run(host='0.0.0.0', port=5001)
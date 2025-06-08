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

async def get_info_nalog(inn: str, semaphore: asyncio.Semaphore, cdp_endpoint: str = "http://localhost:9222") -> dict:
    """Получение данных с pb.nalog.ru для одного ИНН."""
    async with GLOBAL_SEMAPHORE:
        async with async_playwright() as p:
            try:
                logger.info(f"Подключение к CDP по адресу: {cdp_endpoint} для ИНН {inn}")
                browser = await p.chromium.connect_over_cdp(cdp_endpoint)
                context = await browser.new_context()
                start_time = time.time()
                page = await context.new_page()
            except PlaywrightError as e:
                logger.error(f"Ошибка инициализации браузера для ИНН {inn}: {str(e)}")
                return {"status": "error", "message": f"Ошибка инициализации браузера: {str(e)}", "inn": inn}

            max_attempts = 3
            last_error = None

            for attempt in range(1, max_attempts + 1):
                try:
                    url = "https://pb.nalog.ru/index.html"
                    logger.info(f"Попытка {attempt} из {max_attempts}: Загружаю страницу pb.nalog.ru для ИНН {inn}")
                    await page.goto(url, wait_until="load", timeout=20000)

                    # Проверка на пустую страницу
                    if page.url == "about:blank":
                        logger.error(f"Пустая страница для ИНН {inn} на попытке {attempt}")
                        if attempt < max_attempts:
                            await page.wait_for_timeout(3000)
                            continue
                        return {"status": "error", "message": "Пустая страница, возможно, сбой браузера", "inn": inn}

                    content = await page.content()
                    if not content or "<html" not in content.lower():
                        logger.error(f"Пустое содержимое страницы для ИНН {inn} на попытке {attempt}")
                        if attempt < max_attempts:
                            await page.wait_for_timeout(3000)
                            continue
                        return {"status": "error", "message": "Пустое содержимое страницы", "inn": inn}

                    # Проверка на предупреждение о превышении лимита запросов
                    alert = await page.query_selector("div.alert")
                    if alert and "превысили" in (await alert.inner_text()).lower():
                        logger.warning(f"Превышен лимит запросов для ИНН {inn}")
                        return {"status": "error", "message": "Превышен лимит запросов. Пожалуйста, подождите до начала следующего часа.", "inn": inn}

                    # Ожидание поля ввода
                    logger.info(f"Ожидаю поле ввода для ИНН {inn}")
                    await page.wait_for_selector("input#queryAll", timeout=10000)

                    # Проверка активного режима "Общий поиск"
                    active_mode = await page.query_selector("input#m_search-all:checked")
                    if not active_mode:
                        logger.info(f"Переключаюсь на режим 'Общий поиск' для ИНН {inn}")
                        await page.click("label[for='m_search-all']")
                        await page.wait_for_timeout(500)

                    # Ввод ИНН и отправка формы
                    logger.info(f"Ввожу ИНН {inn} в поле")
                    await page.fill("input#queryAll", inn)
                    logger.info(f"Нажимаю кнопку 'Искать' для ИНН {inn}")
                    await page.click("button.btn.btn-warning[type='submit']")

                    # Ожидание результата
                    logger.info(f"Ожидаю результат до 7 секунд для ИНН {inn}")
                    await page.wait_for_timeout(7000)

                    # Проверка на предупреждение о превышении лимита запросов
                    alert = await page.query_selector("div.alert")
                    if alert and "превысили" in (await alert.inner_text()).lower():
                        logger.warning(f"Превышен лимит запросов для ИНН {inn}")
                        return {"status": "error", "message": "Превышен лимит запросов. Пожалуйста, подождите до начала следующего часа.", "inn": inn}

                    # Проверка на отсутствие данных
                    no_data = await page.query_selector("div.no-data:not(.d-none)")
                    if no_data and "По заданным критериям поиска сведений не найдено" in await no_data.inner_text():
                        logger.info(f"Данные для ИНН {inn}: Ничего не найдено")
                        return {"status": "success", "results": {}, "inn": inn}

                    # Извлечение данных с помощью JavaScript
                    result = {"status": "success", "results": {}, "inn": inn}
                    groups_data = await page.evaluate("""
                        () => {
                            const groups = [
                                { key: 'ul', name: 'Организации', id: 'resultul' },
                                { key: 'ip', name: 'Индивидуальные предприниматели', id: 'resultip' },
                                { key: 'upr', name: 'Руководители', id: 'resultupr' },
                                { key: 'uchr', name: 'Учредители', id: 'resultuchr' },
                                { key: 'rdl', name: 'Дисквалификация', id: 'resultrdl' },
                                { key: 'addr', name: 'Адреса ЮЛ', id: 'resultaddr' },
                                { key: 'ogrfl', name: 'Ограничения ФЛ', id: 'resultogrfl' },
                                { key: 'ogrul', name: 'Ограничения ЮЛ', id: 'resultogrul' },
                                { key: 'docul', name: 'Документы ЮЛ', id: 'resultdocul' },
                                { key: 'docip', name: 'Документы ИП', id: 'resultdocip' }
                            ];
                            const result = {};
                            groups.forEach(group => {
                                const groupDiv = document.querySelector(`div#${group.id}`);
                                if (!groupDiv) return;
                                const dataDiv = groupDiv.querySelector('div.data:not(.d-none)');
                                if (!dataDiv) return;
                                const groupData = [];
                                const items = dataDiv.querySelectorAll('dl, tr');
                                items.forEach(item => {
                                    const record = {};
                                    if (item.tagName.toLowerCase() === 'dl') {
                                        const dts = item.querySelectorAll('dt');
                                        const dds = item.querySelectorAll('dd');
                                        dts.forEach((dt, i) => {
                                            const label = dt.textContent.trim().replace(':', '').toLowerCase().replace(/\\s+/g, '_');
                                            const value = dds[i] ? dds[i].textContent.trim() : '';
                                            record[label] = value;
                                        });
                                    } else if (item.tagName.toLowerCase() === 'tr') {
                                        const cells = item.querySelectorAll('th, td');
                                        for (let i = 0; i < cells.length; i += 2) {
                                            const label = cells[i].textContent.trim().replace(':', '').toLowerCase().replace(/\\s+/g, '_');
                                            const value = cells[i + 1] ? cells[i + 1].textContent.trim() : '';
                                            record[label] = value;
                                        }
                                    }
                                    if (Object.keys(record).length > 0) {
                                        groupData.push(record);
                                    }
                                });
                                if (groupData.length > 0) {
                                    result[group.key] = { name: group.name, data: groupData };
                                }
                            });
                            return result;
                        }
                    """)

                    result['results'] = groups_data
                    if not groups_data:
                        logger.info(f"Данные для ИНН {inn}: Ничего не найдено")
                        return {"status": "success", "results": {}, "inn": inn}

                    logger.info(f"Найдено групп данных для ИНН {inn}: {len(groups_data)}")
                    logger.info(f"Обработка ИНН {inn} заняла {time.time() - start_time:.2f} секунд")
                    log_memory_usage()
                    return result

                except PlaywrightError as e:
                    logger.error(f"Попытка {attempt} не удалась для ИНН {inn}: {str(e)}")
                    last_error = str(e)
                    if attempt < max_attempts:
                        logger.info(f"Ожидаю 3 секунды перед повторной попыткой для ИНН {inn}")
                        await page.wait_for_timeout(3000)
                        continue
                    return {"status": "error", "message": f"Ошибка после {max_attempts} попыток: {last_error}", "inn": inn}

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
    tasks = [get_info_nalog(inn, semaphore, cdp_endpoint) for inn in inns]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    logger.info(f"Обработка {len(inns)} ИНН заняла {time.time() - start_time:.2f} секунд")
    log_memory_usage()
    return results

@app.route('/nalog', methods=['POST'])
async def nalog_endpoint():
    """Эндпоинт для получения данных с pb.nalog.ru для одного или нескольких ИНН."""
    data = request.get_json()
    inn = data.get('inn')
    inns = data.get('inns', [])
    cdp_endpoint = data.get('cdp_endpoint', 'http://localhost:9222')

    def is_valid_inn(inn):
        return inn and re.match(r'^\d{10}$|^\d{12}$', inn)

    if inn and is_valid_inn(inn):
        try:
            result = await get_info_nalog(inn, asyncio.Semaphore(1), cdp_endpoint)
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
    app.run(host='0.0.0.0', port=5007)
import asyncio
import logging
import json
from flask import Flask, request, jsonify
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import aiohttp
import base64
import dotenv
import os

# Загрузка переменных окружения
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)),".env")
dotenv.load_dotenv(dotenv_path)

# Ключ API 2Captcha
TWOCAPTCHA_API_KEY = os.getenv("TWOCAPTCHA_API_KEY")

# Пул прокси
proxy_pool = [
    {
        "server": os.getenv("PROXY_SERVER"),
        "username": os.getenv("PROXY_USERNAME"),
        "password": os.getenv("PROXY_PASSWORD")
    },
]

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)
logging.getLogger('httpx').setLevel(logging.WARNING)

app = Flask(__name__)

async def solve_captcha(page, regnum="unknown"):
    """Решает CAPTCHA с помощью сервиса 2Captcha асинхронно."""
    try:
        captcha_img_selector = '#captchaPic img'
        logger.info("Ожидаем появления CAPTCHA-изображения (до 5 секунд)")
        start_time = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start_time < 5:
            try:
                captcha_img = await page.wait_for_selector(captcha_img_selector, state="visible", timeout=1000)
                captcha_src = await captcha_img.get_attribute('src')
                if captcha_src and captcha_src.startswith('data:image/jpeg;base64,'):
                    logger.info("Обнаружено JPEG-изображение CAPTCHA")
                    break
                elif captcha_src and captcha_src.startswith('data:image/gif;base64,'):
                    logger.info("Обнаружен GIF загрузки, ждём 1 секунду")
                    await page.wait_for_timeout(1000)
                else:
                    logger.info("Изображение не JPEG и не GIF, ждём 1 секунду")
                    await page.wait_for_timeout(1000)
            except PlaywrightTimeoutError:
                logger.info("Изображение CAPTCHA не появилось, ждём 1 секунду")
                await page.wait_for_timeout(1000)
        else:
            logger.info("JPEG-изображение CAPTCHA не появилось за 5 секунд")
            return None

        captcha_img = await page.query_selector(captcha_img_selector)
        captcha_src = await captcha_img.get_attribute('src')
        if not captcha_src or 'spinner' in captcha_src.lower() or 'loading' in captcha_src.lower():
            logger.info("Не удалось загрузить CAPTCHA (вероятно, спиннер)")
            return None

        logger.info("Извлекаем изображение CAPTCHA")
        if captcha_src.startswith('data:image/jpeg;base64,'):
            base64_string = captcha_src.split(',')[1]
            captcha_image = base64.b64decode(base64_string)
        else:
            async with aiohttp.ClientSession() as session:
                async with session.get(captcha_src, timeout=10) as response:
                    if response.status != 200:
                        logger.info("Не удалось загрузить изображение CAPTCHA")
                        return None
                    captcha_image = await response.read()

        logger.info("Отправляем CAPTCHA на 2Captcha")
        form_data = aiohttp.FormData()
        form_data.add_field('key', TWOCAPTCHA_API_KEY)
        form_data.add_field('method', 'post')
        form_data.add_field('numeric', '1')
        form_data.add_field('min_len', '5')
        form_data.add_field('max_len', '5')
        form_data.add_field('json', '1')
        form_data.add_field('file', captcha_image, filename='captcha.jpg', content_type='image/jpeg')

        async with aiohttp.ClientSession() as session:
            async with session.post('http://2captcha.com/in.php', data=form_data) as response:
                if response.status != 200:
                    logger.info(f"Ошибка HTTP при отправке CAPTCHA: {response.status}")
                    return None
                result = await response.json()

        if result.get('status') != 1:
            logger.info(f"Ошибка 2Captcha при отправке: {result.get('request')}")
            return None

        captcha_id = result['request']
        logger.info(f"Получен ID CAPTCHA: {captcha_id}")

        max_attempts = 10
        for attempt in range(max_attempts):
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f'http://2captcha.com/res.php?key={TWOCAPTCHA_API_KEY}&action=get&id={captcha_id}&json=1',
                    timeout=10
                ) as response:
                    if response.status != 200:
                        logger.info(f"Ошибка HTTP при получении решения CAPTCHA: {response.status}")
                        continue
                    result = await response.json()
                    if result.get('status') == 1:
                        logger.info(f"CAPTCHA решена: {result['request']}")
                        return result['request']
                    logger.info(f"Ожидаем решения CAPTCHA (попытка {attempt + 1}/{max_attempts})")
                    await asyncio.sleep(5)

        logger.info(f"Не удалось решить CAPTCHA после {max_attempts} попыток")
        return None
    except PlaywrightTimeoutError:
        logger.info("Тайм-аут при ожидании CAPTCHA")
        return None
    except Exception as e:
        logger.info(f"Ошибка при решении CAPTCHA: {str(e)}")
        return None

async def check_captcha(page):
    """Проверяет наличие CAPTCHA."""
    captcha_selector = '#captchaDialog'
    try:
        logger.info("Проверяем наличие CAPTCHA (до 5 секунд)")
        captcha_element = await page.query_selector(captcha_selector)
        if captcha_element and await captcha_element.is_visible():
            logger.info("Обнаружена CAPTCHA")
            return True
    except Exception as e:
        logger.error(f"Ошибка проверки CAPTCHA: {str(e)}")
    logger.info("CAPTCHA не обнаружена")
    return False

async def perform_search(page, vin):
    """Выполняет поиск на сайте ГИБДД и возвращает JSON-результат."""
    try:
        vin_input_selector = '#checkAutoVIN'
        logger.info("Ожидаем поле ввода VIN (до 5 секунд)")
        await page.wait_for_selector(vin_input_selector, state="visible", timeout=5000)
        logger.info(f"Вводим VIN: {vin}")
        await page.fill(vin_input_selector, vin)

        history_button_selector = '//a[@href="#history" and @data-type="history"]'
        logger.info("Ожидаем кнопку истории (до 5 секунд)")
        await page.wait_for_selector(history_button_selector, state="visible", timeout=5000)
        logger.info("Нажимаем кнопку истории")
        await page.click(history_button_selector, timeout=5000)

        if await check_captcha(page):
            logger.info("Ожидаем 3 секунды перед обработкой CAPTCHA")
            await page.wait_for_timeout(3000)
            captcha_solution = await solve_captcha(page, vin)
            if not captcha_solution:
                logger.error("Не удалось решить CAPTCHA")
                return {"status": "error", "message": "Не удалось решить CAPTCHA", "vehicle": {}, "ownership_periods": [], "retry": True}

            captcha_input_selector = 'input[name="captcha_num"]'
            logger.info("Ожидаем поле ввода CAPTCHA (до 5 секунд)")
            await page.wait_for_selector(captcha_input_selector, state="visible", timeout=5000)
            logger.info(f"Вводим решение CAPTCHA: {captcha_solution}")
            await page.fill(captcha_input_selector, captcha_solution)
            logger.info("Ожидаем 3 секунды после ввода CAPTCHA")
            await page.wait_for_timeout(3000)
            logger.info("Отправляем CAPTCHA")
            try:
                await page.evaluate('document.querySelector("#captchaSubmit").click()')
            except Exception as e:
                logger.error(f"Ошибка при отправке CAPTCHA: {str(e)}")
                return {"status": "error", "message": "Ошибка отправки CAPTCHA", "vehicle": {}, "ownership_periods": [], "retry": True}

            try:
                logger.info("Ожидаем скрытия CAPTCHA (до 10 секунд)")
                await page.wait_for_selector('#captchaDialog', state="hidden", timeout=10000)
            except PlaywrightTimeoutError:
                error_message = await page.query_selector('p.check-space.check-message')
                if error_message and "Проверка CAPTCHA не была пройдена" in await error_message.inner_text():
                    logger.error("Ошибка: CAPTCHA не прошла проверку")
                    return {"status": "error", "message": "Ошибка: CAPTCHA не прошла проверку", "vehicle": {}, "ownership_periods": [], "retry": True}
                logger.error("Ошибка отправки CAPTCHA: диалог не скрыт")
                return {"status": "error", "message": "Ошибка отправки CAPTCHA", "vehicle": {}, "ownership_periods": [], "retry": True}

        # Ожидаем появления результатов
        logger.info("Ожидаем блок результатов (до 10 секунд)")
        await page.wait_for_selector('#checkAutoHistory .checkResult', state="visible", timeout=10000)
        history_div = await page.query_selector('#checkAutoHistory')
        if not history_div:
            logger.error("Не удалось найти блок результатов")
            return {"status": "error", "message": "Не удалось найти результаты", "vehicle": {}, "ownership_periods": [], "retry": False}

        # Проверяем наличие ошибок в сообщении
        check_message = await history_div.query_selector('p.check-space.check-message')
        if check_message and await check_message.is_visible():
            message_text = await check_message.inner_text()
            if "Проверка не запрашивалась" in message_text:
                logger.error("Поиск не выполнен: проблема с CAPTCHA или запросом")
                return {"status": "error", "message": "Поиск не выполнен: проблема с CAPTCHA или запросом", "vehicle": {}, "ownership_periods": [], "retry": True}
            elif "По указанному VIN не найдена информация" in message_text:
                logger.info("История регистрации для этого VIN не найдена")
                return {"status": "no_data", "message": "История регистрации для этого VIN не найдена", "vehicle": {}, "ownership_periods": [], "retry": False}
            elif "Проверка CAPTCHA не была пройдена" in message_text:
                logger.error("Ошибка: CAPTCHA не прошла проверку")
                return {"status": "error", "message": "Ошибка: CAPTCHA не прошла проверку", "vehicle": {}, "ownership_periods": [], "retry": True}

        vehicle_data = {}
        ownership_periods = []
        max_parse_attempts = 3
        for attempt in range(max_parse_attempts):
            logger.info(f"Попытка парсинга #{attempt + 1}")

            # Парсинг характеристик транспортного средства
            vehicle_list = await history_div.query_selector('ul.fields-list.vehicle')
            if vehicle_list:
                vehicle_items = await vehicle_list.query_selector_all('li')
                for item in vehicle_items:
                    caption = await item.query_selector('span.caption')
                    field = await item.query_selector('span.field')
                    if caption and field:
                        key = (await caption.inner_text()).replace(':', '').strip().lower().replace(' ', '_')
                        value = (await field.inner_text()).strip()
                        vehicle_data[key] = value

            # Парсинг периодов владения
            periods_list = await history_div.query_selector('ul.ownershipPeriods')
            if periods_list:
                period_items = await periods_list.query_selector_all('li')
                for item in period_items:
                    from_date = await item.query_selector('span.ownershipPeriods-from')
                    to_date = await item.query_selector('span.ownershipPeriods-to')
                    owner_type = await item.query_selector('span.simplePersonType')
                    period = {
                        "from": (await from_date.inner_text()).strip() if from_date else "",
                        "to": (await to_date.inner_text()).strip() if to_date else "",
                        "owner_type": (await owner_type.inner_text()).strip() if owner_type else ""
                    }
                    ownership_periods.append(period)

            if vehicle_data or ownership_periods:
                logger.info("Данные успешно извлечены")
                break
            if attempt < max_parse_attempts - 1:
                logger.info("Ожидаем 5 секунд перед повторной попыткой парсинга")
                await page.wait_for_timeout(5000)

        if not vehicle_data and not ownership_periods:
            logger.error("Не удалось извлечь данные о транспортном средстве")
            return {"status": "error", "message": "Не удалось извлечь данные о транспортном средстве", "vehicle": {}, "ownership_periods": [], "retry": True}

        logger.info("История регистрации найдена")
        return {
            "status": "success",
            "message": "История регистрации найдена",
            "vehicle": vehicle_data,
            "ownership_periods": ownership_periods,
            "retry": False
        }
    except Exception as e:
        logger.error(f"Поиск не удался: {str(e)}")
        return {"status": "error", "message": f"Поиск не удался: {str(e)}", "vehicle": {}, "ownership_periods": [], "retry": False}

async def get_gibdd_info(vin: str) -> dict:
    """Получает информацию о транспортном средстве по VIN с сайта ГИБДД."""
    if not isinstance(vin, str) or len(vin) != 17 or not vin.isalnum():
        logger.error("Недопустимый VIN: должен быть 17 буквенно-цифровых символов")
        return {"status": "error", "message": "Недопустимый VIN", "vehicle": {}, "ownership_periods": []}

    async with async_playwright() as p:
        logger.info("Запускаем браузер")
        browser = await p.chromium.launch(headless=True)
        browser_context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720},
            proxy=proxy_pool[0]
        )
        page = await browser_context.new_page()

        max_retries = 4
        attempt = 1
        result = None

        while attempt <= max_retries:
            try:
                logger.info(f"Попытка {attempt} из {max_retries}: Загружаем сайт ГИБДД")
                await page.goto("https://xn--90adear.xn--p1ai/check/auto", timeout=15000)
                await page.wait_for_load_state("load", timeout=15000)
                result = await perform_search(page, vin)
                if result["status"] == "success" and (result["vehicle"] or result["ownership_periods"]):
                    logger.info("Получен успешный результат")
                    break
                elif result.get("retry", False):
                    logger.info(f"Попытка {attempt} не удалась: {result['message']}. Требуется повторная попытка")
                    if attempt == max_retries:
                        logger.error("Достигнуто максимальное количество попыток")
                        result = {"status": "error", "message": "Не удалось решить CAPTCHA после 4 попыток", "vehicle": {}, "ownership_periods": []}
                        break
                else:
                    logger.info(f"Попытка {attempt} не удалась: {result['message']}. Повторная попытка не требуется")
                    break

            except Exception as e:
                logger.error(f"Попытка {attempt} не удалась: {str(e)}")
                if attempt == max_retries:
                    logger.error("Достигнуто максимальное количество попыток")
                    result = {"status": "error", "message": "Не удалось решить CAPTCHA после 4 попыток", "vehicle": {}, "ownership_periods": []}
                    break

            attempt += 1
            logger.info("Ожидаем 3 секунды перед следующей попыткой")
            await page.wait_for_timeout(3000)

        try:
            logger.info("Закрываем браузер")
            await browser.close()
        except Exception as e:
            logger.error(f"Ошибка при закрытии браузера: {str(e)}")

        return result

@app.route('/gibdd', methods=['POST'])
async def gibdd_endpoint():
    """Эндпоинт для получения данных ГИБДД."""
    data = request.get_json()
    vin = data.get('vin')
    if not vin or not (isinstance(vin, str) and len(vin) == 17 and vin.isalnum()):
        return jsonify({"status": "error", "message": "Недопустимый VIN: должен быть 17 буквенно-цифровых символов"}), 400

    result = await get_gibdd_info(vin)
    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003)
import asyncio
import logging
import re
import random
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
    format='%(message)s',
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

        max_attempts = 3
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
        captcha_element = await page.wait_for_selector(captcha_selector, state="visible", timeout=5000)
        if captcha_element and await captcha_element.is_visible():
            logger.info("Обнаружена CAPTCHA")
            return True
    except PlaywrightTimeoutError:
        logger.info("CAPTCHA не обнаружена")
        return False
    except Exception as e:
        logger.info(f"Ошибка проверки CAPTCHA: {str(e)}")
        return False

async def perform_search(page, regnum, regreg, stsnum):
    """Выполняет поиск штрафов на сайте ГИБДД и возвращает словарь результата."""
    try:
        logger.info("Ожидаем форму #checkFinesContainer (до 3 секунд)")
        await page.wait_for_selector('#checkFinesContainer', state="visible", timeout=3000)

        logger.info("Валидация формата СТС")
        stsnum_match = re.match(r'^(\d{2})([А-Я0-9]{2})(\d{6})$', stsnum)
        if not stsnum_match:
            logger.info("Неверный формат СТС")
            return {"status": "error", "message": "Неверный формат СТС", "data": "", "retry": False}

        max_captcha_attempts = 2
        for captcha_attempt in range(max_captcha_attempts):
            logger.info(f"Попытка ввода данных и CAPTCHA #{captcha_attempt + 1}")

            regnum_input_selector = '#checkFinesRegnum'
            regreg_input_selector = '#checkFinesRegreg'
            stsnum_input_selector = '#checkFinesStsnum'

            try:
                logger.info("Ожидаем поле ввода госномера (до 3 секунд)")
                await page.wait_for_selector(regnum_input_selector, state="visible", timeout=3000)
                regnum_input = await page.query_selector(regnum_input_selector)
                if not (await regnum_input.is_visible() and await regnum_input.is_enabled()):
                    raise Exception("Поле госномера не видимо или не доступно")
                logger.info(f"Вводим госномер: {regnum}")
                await page.type(regnum_input_selector, regnum, delay=100)
            except Exception as e:
                logger.info(f"Ошибка ввода госномера: {str(e)}")
                return {"status": "error", "message": "Не удалось ввести госномер", "data": "", "retry": False}

            try:
                logger.info("Ожидаем поле ввода региона (до 3 секунд)")
                await page.wait_for_selector(regreg_input_selector, state="visible", timeout=3000)
                regreg_input = await page.query_selector(regreg_input_selector)
                if not (await regreg_input.is_visible() and await regreg_input.is_enabled()):
                    raise Exception("Поле региона не видимо или не доступно")
                logger.info(f"Вводим регион: {regreg}")
                await page.type(regreg_input_selector, regreg, delay=100)
            except Exception as e:
                logger.info(f"Ошибка ввода региона: {str(e)}")
                return {"status": "error", "message": "Не удалось ввести регион", "data": "", "retry": False}

            max_input_attempts = 2
            for input_attempt in range(max_input_attempts):
                logger.info(f"Попытка ввода СТС #{input_attempt + 1}")
                try:
                    await page.wait_for_selector(stsnum_input_selector, state="visible", timeout=3000)
                    stsnum_input = await page.query_selector(stsnum_input_selector)
                    if not (await stsnum_input.is_visible() and await stsnum_input.is_enabled()):
                        raise Exception("Поле СТС не видимо или не доступно")
                    logger.info(f"Очищаем и вводим СТС: {stsnum}")
                    await page.evaluate(f'''
                        let input = document.querySelector("{stsnum_input_selector}");
                        input.value = "";
                        input.value = "{stsnum}";
                        input.dispatchEvent(new Event("input"));
                        input.blur();
                    ''')
                    await page.wait_for_timeout(1000)
                    entered_value = await page.evaluate(f'document.querySelector("{stsnum_input_selector}").value')
                    if entered_value == stsnum:
                        logger.info("СТС успешно введён")
                        break
                    else:
                        raise Exception(f"Введённое значение СТС не совпадает: {entered_value}")
                except Exception as e:
                    logger.info(f"Ошибка ввода СТС: {str(e)}")
                    if input_attempt < max_input_attempts - 1:
                        logger.info("Ожидаем 3 секунды перед повторной попыткой ввода СТС")
                        await page.wait_for_timeout(3000)
                    else:
                        logger.info("Не удалось вставить СТС после 2 попыток")
                        return {"status": "error", "message": "Не удалось вставить СТС", "data": "", "retry": False}

            logger.info("Имитируем пользовательские действия")
            await page.mouse.move(random.randint(100, 500), random.randint(100, 500))
            await page.evaluate('window.scrollBy(0, 200)')
            delay = random.randint(1000, 2000)
            logger.info(f"Ожидаем случайную паузу {delay} мс")
            await page.wait_for_timeout(delay)

            check_button_selector = 'a.checker[data-type="fines"]'
            max_click_attempts = 3
            for click_attempt in range(max_click_attempts):
                logger.info(f"Попытка нажатия кнопки проверки #{click_attempt + 1}")
                try:
                    button = await page.wait_for_selector(check_button_selector, state="visible", timeout=3000)
                    is_clickable = await page.evaluate(
                        '(el) => el.offsetParent !== null && window.getComputedStyle(el).visibility !== "hidden" && !el.disabled',
                        button
                    )
                    if not is_clickable:
                        raise Exception("Кнопка проверки не кликабельна")
                    await page.wait_for_timeout(1000)
                    logger.info("Нажимаем кнопку проверки")
                    await page.evaluate('el => el.click()', button)
                    break
                except Exception as e:
                    logger.info(f"Ошибка нажатия кнопки проверки: {str(e)}")
                    if click_attempt < max_click_attempts - 1:
                        logger.info("Ожидаем 3 секунды перед повторной попыткой нажатия")
                        await page.wait_for_timeout(3000)
                    else:
                        logger.info("Не удалось инициировать проверку после 3 попыток")
                        return {"status": "error", "message": "Не удалось инициировать проверку", "data": "",
                                "retry": False}

            logger.info("Проверяем наличие CAPTCHA")
            if not await check_captcha(page):
                logger.info("CAPTCHA не появилась")
                if captcha_attempt < max_captcha_attempts - 1:
                    logger.info("Повторяем попытку ввода данных")
                    continue
                logger.info("CAPTCHA не появилась после 2 попыток")
                return {"status": "error", "message": "CAPTCHA не появилась", "data": "", "retry": True}

            logger.info("Ожидаем 3 секунды перед обработкой CAPTCHA")
            await page.wait_for_timeout(3000)
            logger.info("Решаем CAPTCHA")
            captcha_solution = await solve_captcha(page, regnum)
            if not captcha_solution:
                logger.info("CAPTCHA не решена")
                if captcha_attempt < max_captcha_attempts - 1:
                    logger.info("Повторяем попытку ввода данных")
                    continue
                logger.info("Не удалось решить CAPTCHA после 2 попыток")
                return {"status": "error", "message": "Не удалось решить CAPTCHA", "data": "", "retry": True}

            captcha_input_selector = 'input[name="captcha_num"]'
            logger.info("Ожидаем поле ввода CAPTCHA (до 3 секунд)")
            await page.wait_for_selector(captcha_input_selector, state="visible", timeout=3000)
            logger.info(f"Вводим решение CAPTCHA: {captcha_solution}")
            await page.fill(captcha_input_selector, captcha_solution)
            logger.info("Ожидаем 3 секунды после ввода CAPTCHA")
            await page.wait_for_timeout(3000)
            logger.info("Отправляем CAPTCHA")
            try:
                await page.evaluate('document.querySelector("#captchaSubmit").click()')
            except Exception as e:
                logger.info(f"Ошибка отправки CAPTCHA: {str(e)}")
                if captcha_attempt < max_captcha_attempts - 1:
                    logger.info("Повторяем попытку ввода данных")
                    continue
                logger.info("Ошибка отправки CAPTCHA после 2 попыток")
                return {"status": "error", "message": "Ошибка отправки CAPTCHA", "data": "", "retry": True}

            logger.info("Ожидаем скрытия CAPTCHA-диалога (до 5 секунд)")
            try:
                await page.wait_for_selector('#captchaDialog', state="hidden", timeout=5000)
            except PlaywrightTimeoutError as e:
                logger.info(f"Ошибка отправки CAPTCHA: диалог не скрыт: {str(e)}")
                if captcha_attempt < max_captcha_attempts - 1:
                    logger.info("Повторяем попытку ввода данных")
                    continue
                logger.info("Ошибка отправки CAPTCHA после 2 попыток")
                return {"status": "error", "message": "Ошибка отправки CAPTCHA", "data": "", "retry": True}

            logger.info("Ожидаем сообщения о выполнении запроса (до 5 секунд)")
            try:
                await page.wait_for_selector('p.check-space.check-message:has-text("Выполняется запрос, ждите")',
                                             state="visible", timeout=5000)
                logger.info("Запрос выполняется, ждём 3 секунды")
                await page.wait_for_timeout(3000)
            except PlaywrightTimeoutError:
                logger.info("Сообщение о запросе не появилось")

            logger.info("Ожидаем результатов #checkFinesSheet (до 10 секунд)")
            await page.wait_for_selector('#checkFinesSheet', state="attached", timeout=10000)
            fines_sheet = await page.query_selector('#checkFinesSheet')
            if not fines_sheet:
                logger.info("Результаты не найдены")
                if captcha_attempt < max_captcha_attempts - 1:
                    logger.info("Повторяем попытку ввода данных")
                    continue
                logger.info("Не удалось найти результаты после 2 попыток")
                return {"status": "error", "message": "Не удалось найти результаты", "data": "", "retry": False}

            logger.info("Парсим результаты из #checkFinesSheet")
            result_text = []

            check_fines_section = await fines_sheet.query_selector('#checkFines')
            if check_fines_section:
                message_elements = await check_fines_section.query_selector_all('p.check-space.check-message')
                for elem in message_elements:
                    text = await elem.inner_text()
                    result_text.append(text.strip())

                result_elements = await check_fines_section.query_selector_all('div.checkResult')
                for elem in result_elements:
                    text = await elem.inner_text()
                    if text.strip():
                        result_text.append(text.strip())

            static_text_to_remove = [
                "Обращаем Ваше внимание!",
                "Обжалование постановлений по делам об административных правонарушениях осуществляется в соответствии с 30 главой КоАП РФ. Данное право может быть реализовано путем направления жалобы, которая подписывается лицом, в отношении которого вынесено постановление.",
                "В соответствии со статьей 30.2 КоАП РФ жалоба на постановление по делу об административном правонарушении в случае фиксации этого административного правонарушения работающими в автоматическом режиме специальными техническими средствами, может быть также подана в форме электронного документа, с использованием Единого портала государственных и муниципальных услуг.",
                "Посредством сервиса приема обращений на официальном сайте Госавтоинспекции жалоба не может быть направлена (Решение Верховного Суда РФ от 27 сентября 2018 г. № 53-ААД18-10)."
            ]

            parsed_result = "\n".join([text for text in result_text if text not in static_text_to_remove])

            if not parsed_result.strip():
                logger.info("Получен пустой результат после фильтрации")
                if captcha_attempt < max_captcha_attempts - 1:
                    logger.info("Повторяем попытку ввода данных")
                    continue
                logger.info("Не удалось получить результаты после 2 попыток")
                return {"status": "error", "message": "Не удалось получить результаты", "data": "", "retry": False}

            logger.info(f"Извлечённый текст результата: {parsed_result[:100]}...")
            return {"status": "success", "message": "Результаты получены", "data": parsed_result, "retry": False}

    except Exception as e:
        logger.info(f"Поиск не удался: {str(e)}")
        return {"status": "error", "message": f"Поиск не удался: {str(e)}", "data": "", "retry": False}

async def get_fines_info(regnum: str, regreg: str, stsnum: str) -> dict:
    """Проверяет штрафы по госномеру, региону и СТС на сайте ГИБДД."""
    logger.info(f"Начало поиска штрафов: {regnum} {regreg} {stsnum}")
    if not isinstance(regnum, str) or not re.match(r'^[А-Я0-9]{6}$', regnum):
        logger.info("Неверный формат госномера")
        return {"status": "error", "message": "Неверный формат госномера (6 символов, буквы кириллица и цифры)", "data": ""}

    if not isinstance(regreg, str) or not regreg.isdigit():
        logger.info("Неверный формат региона")
        return {"status": "error", "message": "Неверный формат региона (только цифры)", "data": ""}

    if not isinstance(stsnum, str) or not re.match(r'^\d{2}[А-Я0-9]{2}\d{6}$', stsnum):
        logger.info("Неверный формат СТС")
        return {"status": "error", "message": "Неверный формат СТС (99АА999999)", "data": ""}

    async with async_playwright() as p:
        logger.info("Запуск браузера")
        browser = await p.chromium.launch(headless=True)
        browser_context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720},
            proxy=proxy_pool[0]
        )

        max_retries = 4
        attempt = 1
        result = None

        while attempt <= max_retries:
            logger.info(f"Попытка {attempt} из {max_retries}: Загружаем сайт ГИБДД")
            page = await browser_context.new_page()
            try:
                await page.goto("https://xn--90adear.xn--p1ai/check/fines", timeout=30000)
                await page.wait_for_load_state("load", timeout=30000)
                result = await perform_search(page, regnum, regreg, stsnum)
                logger.info(f"Результат поиска: {result['message'][:100]}...")
                if result["status"] == "success":
                    logger.info("Получен успешный результат")
                    await page.close()
                    break
                elif result.get("retry", False):
                    logger.info(f"Попытка {attempt} не удалась: {result['message']}. Требуется повторная попытка")
                    if attempt == max_retries:
                        logger.info("Достигнуто максимальное количество попыток")
                        result = {"status": "error", "message": "Не удалось решить CAPTCHA после 4 попыток", "data": "",
                                  "retry": False}
                        await page.close()
                        break
                else:
                    logger.info(f"Попытка {attempt} не удалась: {result['message']}. Повторная попытка не требуется")
                    await page.close()
                    break

            except Exception as e:
                logger.info(f"Ошибка загрузки страницы: {str(e)}")
                await page.close()
                if attempt == max_retries:
                    logger.info("Достигнуто максимальное количество попыток")
                    result = {"status": "error", "message": "Достигнуто максимальное количество попыток", "data": "",
                              "retry": False}
                    break

            attempt += 1
            logger.info("Ожидаем 3 секунды перед следующей попыткой")
            await asyncio.sleep(3)

        logger.info("Закрываем браузер")
        try:
            await browser.close()
            logger.info("Браузер закрыт")
        except Exception as e:
            logger.info(f"Ошибка при закрытии браузера: {str(e)}")

        return result

@app.route('/fines', methods=['POST'])
async def fines_endpoint():
    """Эндпоинт для получения данных о штрафах."""
    data = request.get_json()
    regnum = data.get('regnum')
    regreg = data.get('regreg')
    stsnum = data.get('stsnum')

    if not all([regnum, regreg, stsnum]):
        return jsonify({"status": "error", "message": "Не указаны все параметры: regnum, regreg, stsnum"}), 400

    result = await get_fines_info(regnum, regreg, stsnum)
    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5004)
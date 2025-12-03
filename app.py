import os
import time
import asyncio
import aiohttp
from flask import Flask, request, jsonify
from NexarClient import NexarClient
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from flask_cors import CORS
import requests

load_dotenv()

app = Flask(__name__)

client_app = os.getenv("CLIENT_APP")
client_tellur = os.getenv("CLIENT_TELLUR")

CORS(app, resources={
    r"/api/process": {
        "origins": [client_app, client_tellur],
        "methods": ["POST"],
        "allow_headers": ["Content-Type", "Authorization"],
        "supports_credentials": True
    }
})

#@app.after_request
#def add_cors_headers(response):
#    response.headers["Access-Control-Allow-Origin"] = client_tellur
#    response.headers["Access-Control-Allow-Credentials"] = "true"
#    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
#    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
#    return response

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "app.log")

formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s - %(message)s')
file_handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=5)
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

gunicorn_logger = logging.getLogger("gunicorn.error")
if getattr(gunicorn_logger, 'handlers', None):
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    app.logger.propagate = False

@app.route('/api/process', methods=['POST'])
def process_bom():
    data = request.get_json(force=True)
    app.logger.info(f"Получен запрос на /api/process: {data}")
    mapping = data.get("mapping", {})
    mode = data.get("mode", "full")
    rows = data.get("data", [])

    if not rows and not data.get("q"):
        return jsonify({"error": "Нет данных или мэппинга"}), 400

    mpn_list = []
    if rows:
        keys = list(mapping.keys())
        values = list(mapping.values())
        try:
            part_index = int(keys[values.index("partNumber")])
        except Exception:
            return jsonify({"error": "Не удалось распознать индекс partNumber в mapping"}), 400

        quantity_index = None
        if "quantity" in values:
            try:
                quantity_index = int(keys[values.index("quantity")])
            except Exception:
                quantity_index = None

        start_index = 1 if any(str(cell).lower() in ["mpn", "partnumber", "количество", "quantity", "manufacturer", "производитель"] for cell in rows[0]) else 0

        for row in rows[start_index:]:
            if len(row) > part_index:
                mpn = str(row[part_index]).strip()
                if not mpn:
                    continue
                quantity = None
                if quantity_index is not None and len(row) > quantity_index:
                    try:
                        quantity = int(row[quantity_index])
                    except Exception:
                        quantity = None
                mpn_list.append({"mpn": mpn, "quantity": quantity})

    elif data.get("q"):
        mpn_list.append({"mpn": data["q"], "quantity": None})

    app.logger.info(f"Сформирован список MPN для обработки: {mpn_list}")

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        nexar_data = loop.run_until_complete(process_all_mpn(mpn_list, mode))
    except Exception as e:
        app.logger.error(f"Ошибка при обработке: {str(e)}", exc_info=True)
        return jsonify({"status": "error", "message": str(e) or "Неизвестная ошибка при обработке"}), 500

    return jsonify({"data": nexar_data})


async def process_all_mpn(mpn_list, mode, chunk_size=15, max_retries=3):
    clientId = os.getenv("CLIENT_ID")
    clientSecret = os.getenv("CLIENT_SECRET")
    nexar = NexarClient(clientId, clientSecret)
    ALLOWED_SELLERS = [
        "Mouser", "Digi-Key", "Arrow", "TTI", "ADI",
        "Coilcraft", "Rochester", "Verical", "Texas Instruments", "MINICIRCUITS"
    ]

    output_data = []

    async def partial_request_variations(mpn_item):
        gqlQuery = '''
        query Search ($q: String!) {
          supSearch(q: $q, limit: 50, currency: "USD") {
            results {
              part { mpn name }
            }
          }
        }
        '''
        variables = {"q": mpn_item["mpn"]}

        for attempt in range(1, max_retries + 1):
            try:
                results = nexar.get_query(gqlQuery, variables)
                app.logger.info(f"Ответ на Partial-запрос от Nexar для {mpn_item['mpn']} : {results}")
                break
            except Exception as e:
                wait = 2 ** (attempt - 1)
                app.logger.warning(f"Partial-запрос Nexar ошибка ({mpn_item['mpn']}, попытка {attempt}/{max_retries}): {e}. Жду {wait}s.")
                await asyncio.sleep(wait)
        else:
            return [mpn_item["mpn"]]

        items = (results.get("supSearch") or {}).get("results") or []

        variants = []
        for item in items:
            part = item.get("part")
            if part and part.get("mpn"):
                variants.append(part["mpn"])
        return variants or [mpn_item["mpn"]]

    partial_tasks = [partial_request_variations(item) for item in mpn_list]
    all_variants_lists = await asyncio.gather(*partial_tasks)

    mapping = {
        item["mpn"]: {
            "variants": variants,
            "quantity": item.get("quantity"),
            "results": {}
        }
        for item, variants in zip(mpn_list, all_variants_lists)
    }

    multi_mpn_list = [{"mpn": v} for sublist in all_variants_lists for v in sublist]

    for i in range(0, len(multi_mpn_list), chunk_size):
        chunk = multi_mpn_list[i:i + chunk_size]
        variables = {"queries": [{"mpn": item["mpn"]} for item in chunk]}
        gqlQuery = '''
        query csvDemo($queries: [SupPartMatchQuery!]!) {
          supMultiMatch(currency: "USD", queries: $queries) {
            parts {
              mpn
              name
              category { id name }
              images { url }
              descriptions { text }
              manufacturer { id name }
              sellers {
                company { id name isVerified homepageUrl }
                offers { inventoryLevel prices { quantity currency convertedPrice convertedCurrency } }
              }
            }
          }
        }
        '''

        for attempt in range(1, max_retries + 1):
            try:
                results = nexar.get_query(gqlQuery, variables)
                app.logger.info(f"Ответ от Nexar (чанк {i // chunk_size + 1}): {results}")
                break
            except Exception as e:
                wait = 2 ** (attempt - 1)
                app.logger.warning(f"Nexar API ошибка (попытка {attempt}/{max_retries}): {e}. Жду {wait}s.")
                await asyncio.sleep(wait)
        else:
            app.logger.error(f"Nexar API не ответил корректно после {max_retries} попыток для чанка {i // chunk_size + 1}")
            continue

        multi_res = results.get("supMultiMatch", [])
        if isinstance(multi_res, dict):
            multi_res = [multi_res]

        for block in multi_res:
            for part in block.get("parts", []):
                mpn_found = part.get("mpn")
                if not mpn_found:
                    continue

                for req, data in mapping.items():
                    if mpn_found in data["variants"]:
                        data["results"][mpn_found] = part
                        break

    for requested_mpn, data in mapping.items():
        qty = data["quantity"]

        if not data["results"]:
            output_data.append({
                "requested_mpn": requested_mpn,
                "mpn": None,
                "status": "Не найдено"
            })
            continue

        for found_mpn, part in data["results"].items():
            rows = process_part(
                part=part,
                original_mpn=requested_mpn,
                found_mpn=found_mpn,
                ALLOWED_SELLERS=ALLOWED_SELLERS,
                requested_quantity=qty
            )
            output_data.extend(rows)

    if mode == "short":
        rate = get_usd_to_rub_rate()
        short_output = []
        for item in output_data:
            price_rub = round(item["price"] * rate, 2) if item.get("price") else None
            short_output.append({
                "requested_mpn": item["requested_mpn"],
                "mpn": item.get("mpn"),
                "manufacturer": item.get("manufacturer"),
                "requested_quantity": item.get("requested_quantity"),
                "stock": item.get("stock"),
                "price": price_rub,
                "currency": "RUB" if price_rub else None,
                "status": item.get("status")
            })
        output_data = short_output

    return output_data


def process_part(part, original_mpn, found_mpn, ALLOWED_SELLERS, requested_quantity=None):

    output_records = []

    original_mpn = original_mpn or ""
    part_name = part.get("name") or ""
    manufacturer_node = part.get("manufacturer") or {}
    category_node = part.get("category") or {}
    images = part.get("images") or []
    descriptions = part.get("descriptions") or []
    sellers = part.get("sellers") or []

    # manufacturer
    if isinstance(manufacturer_node, dict):
        manufacturer_id = manufacturer_node.get("id")
        manufacturer_name = manufacturer_node.get("name")
    else:
        manufacturer_id = None
        manufacturer_name = str(manufacturer_node)

    # category
    category_id = category_node.get("id")
    category_name = category_node.get("name")

    # image URL (берём первую)
    image_url = images[0]["url"] if images and isinstance(images[0], dict) else None

    # description (тоже первую)
    description = descriptions[0]["text"] if descriptions and isinstance(descriptions[0], dict) else None


    # === Проходим всех продавцов ===
    for seller in sellers:
        company = seller.get("company") or {}
        seller_name = company.get("name")
        seller_id = company.get("id")
        seller_verified = company.get("isVerified")
        seller_homepageUrl = company.get("homepageUrl")

        if not seller_name:
            continue

        if ALLOWED_SELLERS and seller_name not in ALLOWED_SELLERS:
            continue

        offers = seller.get("offers") or []

        # === Проходим офферы ===
        for offer in offers:
            stock = offer.get("inventoryLevel")
            prices = offer.get("prices") or []

            # цены внутри оффера
            for price in prices:
                base_price = price.get("convertedPrice")
                currency = price.get("convertedCurrency") or price.get("currency")
                offer_quantity = price.get("quantity")

                # защита от кривых данных
                try:
                    base_price = float(base_price)
                except:
                    base_price = None

                # Ценообразование
                delivery_coef = 1.27
                markup = 1.18

                if base_price:
                    target_price_purchasing = base_price * 0.82
                    cost_with_delivery = target_price_purchasing + delivery_coef
                    target_price_sales = target_price_purchasing + delivery_coef + markup
                else:
                    target_price_purchasing = None
                    cost_with_delivery = None
                    target_price_sales = None

                output_records.append({
                    "requested_mpn": original_mpn,
                    "mpn": found_mpn,
                    "manufacturer": manufacturer_name,
                    "manufacturer_id": manufacturer_id,
                    "manufacturer_name": manufacturer_name,

                    "seller_id": seller_id,
                    "seller_name": seller_name,
                    "seller_verified": seller_verified,
                    "seller_homepageUrl": seller_homepageUrl,

                    "stock": stock,
                    "offer_quantity": offer_quantity,
                    "price": base_price,
                    "currency": currency,

                    "category_id": category_id,
                    "category_name": category_name,
                    "image_url": image_url,
                    "description": description,

                    "requested_quantity": requested_quantity,
                    "status": "Найдено",

                    "delivery_coef": delivery_coef,
                    "markup": markup,
                    "target_price_purchasing": round(target_price_purchasing, 2) if target_price_purchasing else None,
                    "cost_with_delivery": round(cost_with_delivery, 2) if cost_with_delivery else None,
                    "target_price_sales": round(target_price_sales, 2) if target_price_sales else None
                })

    return output_records

def get_usd_to_rub_rate_cached():
    url = "https://api.exchangerate.host/latest?base=USD&symbols=RUB"
    r = requests.get(url, timeout=5)
    rate = r.json()["rates"]["RUB"]
    get_usd_to_rub_rate_cached.last_update = time.time()
    return rate

USD_RUB_RATE = None
USD_RUB_LAST_UPDATE = 0

def get_usd_to_rub_rate():
    global USD_RUB_RATE, USD_RUB_LAST_UPDATE
    now = time.time()
    # обновляем не чаще, чем раз в 6 часов
    if not USD_RUB_RATE or now - USD_RUB_LAST_UPDATE > 6 * 3600:
        try:
            url = "https://api.exchangerate.host/latest?base=USD&symbols=RUB"
            r = requests.get(url, timeout=5)
            USD_RUB_RATE = r.json()["rates"]["RUB"]
            USD_RUB_LAST_UPDATE = now
            app.logger.info(f"Курс USD→RUB обновлён: {USD_RUB_RATE}")
        except Exception as e:
            app.logger.error(f"Ошибка при получении курса валют: {e}")
            # fallback на старый курс
            USD_RUB_RATE = USD_RUB_RATE or 100.0
    return USD_RUB_RATE

if __name__ == '__main__':
    host = os.getenv("HOST", os.getenv("HOST"))
    port = int(os.getenv("PORT", os.getenv("PORT")))
    app.run(host=host, port=port)
import os
import time
import asyncio
from api.NexarClient import NexarClient
import requests
from dotenv import load_dotenv
import logging

load_dotenv()

async def process_all_mpn(mpn_list, mode, logger, chunk_size=15, max_retries=3):
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
                logger.info(f"Ответ на Partial-запрос от Nexar для {mpn_item['mpn']} : {results}")
                break
            except Exception as e:
                wait = 2 ** (attempt - 1)
                logger.warning(f"Partial-запрос Nexar ошибка ({mpn_item['mpn']}, попытка {attempt}/{max_retries}): {e}. Жду {wait}s.")
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
            query PartQuery($queries: [PartMatchQuery!]!) {
                matchParts(queries: $queries) {
                    parts {
                        mpn
                        manufacturer { name }
                        offers {
                            seller {
                                company {
                                    name
                                    isVerified
                                    homepageUrl
                                }
                            }
                            inventoryLevel
                            prices {
                                quantity
                                price
                                currency
                            }
                        }
                    }
                }
            }
        '''

        for attempt in range(1, max_retries + 1):
            try:
                results = nexar.get_query(gqlQuery, variables)
                logger.info(f"Ответ от Nexar (чанк {i // chunk_size + 1}): {results}")
                break
            except Exception as e:
                wait = 2 ** (attempt - 1)
                logger.warning(f"Nexar API ошибка (попытка {attempt}/{max_retries}): {e}. Жду {wait}s.")
                await asyncio.sleep(wait)
        else:
            logger.error(f"Nexar API не ответил корректно после {max_retries} попыток для чанка {i // chunk_size + 1}")
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

                for v in data["variants"]:
                    if mpn_found.startswith(v) or v.startswith(mpn_found):
                        data["results"][mpn_found] = part
                        break

                if mpn_found in data["results"]:
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
        rate = get_usd_to_rub_rate(logger)
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
    offers = part.get("offers") or []

    for offer in offers:
        seller_node = offer.get("seller") or {}
        company = seller_node.get("company") or {}

        seller_name = company.get("name")
        seller_id = company.get("id")
        seller_verified = company.get("isVerified")
        seller_homepageUrl = company.get("homepageUrl")

        if not seller_name:
            continue

        if ALLOWED_SELLERS and seller_name not in ALLOWED_SELLERS:
            continue

        stock = offer.get("inventoryLevel")
        prices = offer.get("prices") or []

        price_breaks = []

        for price in prices:
            try:
                base_price = float(price.get("price"))
            except:
                continue

            currency = price.get("currency")
            offer_quantity = price.get("quantity")

            delivery_coef = 1.27
            markup = 1.18

            target_price_purchasing = base_price * 0.82
            cost_with_delivery = target_price_purchasing + delivery_coef
            target_price_sales = target_price_purchasing + delivery_coef + markup

            price_breaks.append({
                "quantity": offer_quantity,
                "price": base_price,
                "currency": currency,
                "target_price_purchasing": round(target_price_purchasing, 2),
                "cost_with_delivery": round(cost_with_delivery, 2),
                "target_price_sales": round(target_price_sales, 2)
            })

        # если нет валидных цен — пропускаем оффер
        if not price_breaks:
            continue

        best_price = min(price_breaks, key=lambda x: x["price"])

        output_records.append({
            "requested_mpn": original_mpn,
            "mpn": found_mpn,
            "manufacturer": manufacturer_name,
            "manufacturer_id": manufacturer_id,

            "seller_id": seller_id,
            "seller_name": seller_name,
            "seller_verified": seller_verified,
            "seller_homepageUrl": seller_homepageUrl,

            "stock": stock,

            "priceBreaks": price_breaks,
            "price": best_price["price"],
            "currency": best_price["currency"],

            "category_id": category_id,
            "category_name": category_name,
            "image_url": image_url,
            "description": description,

            "requested_quantity": requested_quantity,
            "status": "Найдено"
        })

    if not output_records:
        return [{
            "requested_mpn": original_mpn,
            "mpn": None,
            "status": "Нет офферов от разрешённых продавцов",
            "manufacturer": None,
            "requested_quantity": requested_quantity,
            "stock": None,
            "price": None,
            "currency": None
        }]

    return output_records


def get_usd_to_rub_rate_cached():
    url = "https://api.exchangerate.host/latest?base=USD&symbols=RUB"
    r = requests.get(url, timeout=5)
    rate = r.json()["rates"]["RUB"]
    get_usd_to_rub_rate_cached.last_update = time.time()
    return rate

USD_RUB_RATE = None
USD_RUB_LAST_UPDATE = 0

def get_usd_to_rub_rate(logger):
    global USD_RUB_RATE, USD_RUB_LAST_UPDATE
    now = time.time()
    # обновляем не чаще, чем раз в 6 часов
    if not USD_RUB_RATE or now - USD_RUB_LAST_UPDATE > 6 * 3600:
        try:
            url = "https://api.exchangerate.host/latest?base=USD&symbols=RUB"
            r = requests.get(url, timeout=5)
            USD_RUB_RATE = r.json()["rates"]["RUB"]
            USD_RUB_LAST_UPDATE = now
            logger.info(f"Курс USD→RUB обновлён: {USD_RUB_RATE}")
        except Exception as e:
            logger.error(f"Ошибка при получении курса валют: {e}")
            # fallback на старый курс
            USD_RUB_RATE = USD_RUB_RATE or 100.0
    return USD_RUB_RATE

logger = logging.getLogger(__name__)

# НОВАЯ функция для RQ-задачи
def run_nexar_task(mpn_list, mode):
    """
    Синхронная обертка для асинхронной логики,
    которая будет запускаться RQ воркером.
    """
    # Создаем временный логгер для задачи, чтобы не зависеть от logger Flask
    task_logger = logger
    task_logger.setLevel(logging.INFO)

    # Запуск асинхронной логики
    try:
        results = asyncio.run(process_all_mpn(mpn_list, mode, task_logger))
        return {
            "status": "COMPLETED",
            "result": results
        }
    except Exception as e:
        task_logger.error(f"Ошибка при выполнении задачи Nexar: {e}", exc_info=True)
        return {
            "status": "FAILED",
            "error": str(e)
        }
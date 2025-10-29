import os
import time
from flask import Flask, request, jsonify
from NexarClient import NexarClient
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from flask_cors import CORS

load_dotenv()

app = Flask(__name__)

client_app = os.getenv("CLIENT_APP")
CORS(app, resources={r"/api/process": {"origins": client_app}})

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

# Настройка логирования
log_file = os.path.join(log_dir, "app.log")

handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=5)  # 5MB на файл, 5 бэкапов
formatter = logging.Formatter(
    '%(asctime)s [%(levelname)s] %(name)s - %(message)s'
)
handler.setFormatter(formatter)

# Создаём root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # DEBUG — максимально подробный уровень
logger.addHandler(handler)


# Также можно выводить логи в консоль
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

@app.route('/api/process', methods=['POST'])
def process_bom():
    data = request.get_json()
    app.logger.info(f"Получен запрос на /api/process: {data}")  # лог запроса
    mapping = data.get("mapping", {})
    rows = data.get("data", [])

    if not rows or not mapping:
        return jsonify({"error": "Нет данных или мэппинга"}), 400

    part_index = int(list(mapping.keys())[list(mapping.values()).index("partNumber")])
    quantity_index = (
        int(list(mapping.keys())[list(mapping.values()).index("quantity")])
        if "quantity" in mapping.values()
        else None
    )

    mpn_list = []
    for row in rows[1:]:
        if len(row) > part_index:
            mpn = str(row[part_index]).strip()
            quantity = int(row[quantity_index]) if quantity_index is not None and len(row) > quantity_index and str(row[quantity_index]).isdigit() else None
            mpn_list.append({"mpn": mpn, "quantity": quantity})

    app.logger.info(f"Сформирован список MPN для обработки: {mpn_list}")

    try:
        nexar_data = process_chunk(mpn_list)
    except Exception as e:
        app.logger.error(f"Ошибка при обработке: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

    app.logger.info(f"Бэкенд возвращает данные: {nexar_data}")
    return jsonify({"data": nexar_data})


def process_chunk(mpn_list, chunk_size=10, max_retries=3):
    """
    Обрабатывает список MPN через Nexar API по чанкам с retry и экспоненциальным backoff.
    """
    gqlQuery = '''
    query csvDemo ($queries: [SupPartMatchQuery!]!) {
      supMultiMatch (
        currency: "EUR",
        queries: $queries
      ) {
        parts {
          mpn
          name
          sellers {
            company {
              id
              name
            }
            offers {
              inventoryLevel
              prices {
                quantity
                convertedPrice
                convertedCurrency
              }
            }
          }
        }
      }
    }
    '''

    clientId = os.getenv("CLIENT_ID")
    clientSecret = os.getenv("CLIENT_SECRET")
    nexar = NexarClient(clientId, clientSecret)

    ALLOWED_SELLERS = ["Mouser", "Digi-Key", "Arrow", "TTI", "ADI",
                       "Coilcraft", "Rochester", "Verical", "Texas Instruments", "MINICIRCUITS"]

    output_data = []

    # Разбиваем на чанки
    for i in range(0, len(mpn_list), chunk_size):
        chunk = mpn_list[i:i + chunk_size]
        queries = [{"mpn": item["mpn"]} for item in chunk]
        variables = {"queries": queries}

        app.logger.info(f"Отправка GraphQL запроса к Nexar (чанк {i // chunk_size + 1}): {queries}")

        # Retry с экспоненциальным backoff
        for attempt in range(1, max_retries + 1):
            try:
                results = nexar.get_query(gqlQuery, variables)
                app.logger.info(f"Ответ от Nexar (чанк {i // chunk_size + 1}): {results}")
                break
            except Exception as e:
                wait = 2 ** (attempt - 1)
                msg = str(e)
                app.logger.warning(f"Nexar API ошибка (попытка {attempt}/{max_retries}): {msg}. Жду {wait}s перед повтором.")
                time.sleep(wait)
        else:
            app.logger.error(f"Nexar API не ответил корректно после {max_retries} попыток для чанка {i // chunk_size + 1}")
            # добавляем статус ошибки для каждого MPN в чанке
            for item in chunk:
                output_data.append({
                    "mpn": item["mpn"],
                    "manufacturer": None,
                    "seller_id": None,
                    "seller_name": None,
                    "stock": None,
                    "offer_quantity": None,
                    "price": None,
                    "requested_quantity": item.get("quantity"),
                    "status": "Ошибка Nexar"
                })
            continue

        # Обработка успешного ответа
        for query, item in zip(results.get("supMultiMatch", []), chunk):
            mpn = item["mpn"]
            qty = item.get("quantity")
            parts = query.get("parts", [])

            if not parts:
                output_data.append({
                    "mpn": mpn,
                    "manufacturer": None,
                    "seller_id": None,
                    "seller_name": None,
                    "stock": None,
                    "offer_quantity": None,
                    "price": None,
                    "requested_quantity": qty,
                    "status": "Не найдено"
                })
                continue

            for part in parts:
                part_name = part.get("name", "")
                manufacturer = part_name.rsplit(' ', 1)[0]
                sellers = part.get("sellers", [])

                for seller in sellers:
                    seller_name = seller.get("company", {}).get("name", "")
                    seller_id = seller.get("company", {}).get("id", "")

                    if ALLOWED_SELLERS and seller_name not in ALLOWED_SELLERS:
                        app.logger.debug(f"Продавец {seller_name} исключён (не в списке разрешённых).")
                        continue

                    offers = seller.get("offers", [])
                    for offer in offers:
                        stock = offer.get("inventoryLevel", "")
                        prices = offer.get("prices", [])
                        for price in prices:
                            output_data.append({
                                "mpn": mpn,
                                "manufacturer": manufacturer,
                                "seller_id": seller_id,
                                "seller_name": seller_name,
                                "stock": stock,
                                "offer_quantity": price.get("quantity", ""),
                                "price": price.get("convertedPrice", ""),
                                "requested_quantity": qty,
                                "status": "success"
                            })

            if not any(d["mpn"] == mpn for d in output_data):
                output_data.append({
                    "mpn": mpn,
                    "manufacturer": None,
                    "seller_id": None,
                    "seller_name": None,
                    "stock": None,
                    "offer_quantity": None,
                    "price": None,
                    "requested_quantity": qty,
                    "status": "Не найдено"
                })

    app.logger.info(f"Сформированный output_data: {output_data}")
    return output_data


if __name__ == '__main__':
    app.run(host=os.getenv("HOST"), port=os.getenv("PORT"))
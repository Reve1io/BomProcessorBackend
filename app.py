import os
from flask import Flask, request, jsonify
from NexarClient import NexarClient
from config import CLIENT_ID, CLIENT_SECRET
import logging
from dotenv import load_dotenv
from flask_cors import CORS

load_dotenv()

app = Flask(__name__)

client_app = os.getenv("CLIENT_APP")
CORS(app, resources={r"/process": {"origins": client_app}})

@app.route('/process', methods=['POST'])
def process_bom():
    data = request.get_json()
    mapping = data.get("mapping", {})
    rows = data.get("data", [])

    if not rows or not mapping:
        return jsonify({"error": "Нет данных или мэппинга"}), 400

    # Находим индексы колонок
    part_index = int(list(mapping.keys())[list(mapping.values()).index("partNumber")])
    quantity_index = (
        int(list(mapping.keys())[list(mapping.values()).index("quantity")])
        if "quantity" in mapping.values()
        else None
    )

    # Собираем список MPN с количеством
    mpn_list = []
    for row in rows[1:]:
        if len(row) > part_index:
            mpn = str(row[part_index]).strip()
            quantity = int(row[quantity_index]) if quantity_index is not None and len(row) > quantity_index and str(row[quantity_index]).isdigit() else None
            mpn_list.append({"mpn": mpn, "quantity": quantity})

    app.logger.info(f"MPN для обработки: {mpn_list}")

    # Обращаемся к Nexar API
    try:
        nexar_data = process_chunk(mpn_list)
    except Exception as e:
        app.logger.error(f"Ошибка при обработке: {str(e)}")
        return jsonify({"error": str(e)}), 500

    return jsonify({"data": nexar_data})


def process_chunk(mpn_list):
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

    # Формируем GraphQL-запрос
    queries = [{"mpn": item["mpn"]} for item in mpn_list]
    variables = {"queries": queries}

    try:
        results = nexar.get_query(gqlQuery, variables)
    except Exception as e:
        logging.error(f"Ошибка при GraphQL-запросе: {str(e)}")
        raise

    # Обрабатываем результаты
    output_data = []
    for query, item in zip(results.get("supMultiMatch", []), mpn_list):
        mpn = item["mpn"]
        qty = item.get("quantity", None)
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

    return output_data


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5001)
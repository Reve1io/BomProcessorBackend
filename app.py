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

    # Извлекаем список MPN из данных
    part_index = int(list(mapping.keys())[list(mapping.values()).index("partNumber")])
    mpns = [row[part_index] for row in rows[1:] if len(row) > part_index]

    app.logger.info(f"MPN для обработки: {mpns}")

    # Обращаемся к Nexar API
    try:
        nexar_data = process_chunk(mpns)
    except Exception as e:
        app.logger.error(f"Ошибка при обработке: {str(e)}")
        return jsonify({"error": str(e)}), 500

    return jsonify({"data": nexar_data})

def process_chunk(mpns):
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

    queries = [{"mpn": str(mpn)} for mpn in mpns]
    variables = {"queries": queries}

    try:
        results = nexar.get_query(gqlQuery, variables)
    except Exception as e:
        logging.error(f"Ошибка при GraphQL-запросе: {str(e)}")
        raise

    output_data = []
    for query, mpn in zip(results.get("supMultiMatch", []), mpns):
        parts = query.get("parts", [])
        for part in parts:
            part_name = part.get("name", "")
            part_manufacturer = part_name.rsplit(' ', 1)[0]
            sellers = part.get("sellers", [])
            for seller in sellers:
                seller_name = seller.get("company", {}).get("name", "")
                seller_id = seller.get("company", {}).get("id", "")
                offers = seller.get("offers", [])
                for offer in offers:
                    stock = offer.get("inventoryLevel", "")
                    prices = offer.get("prices", [])
                    for price in prices:
                        quantity = price.get("quantity", "")
                        converted_price = price.get("convertedPrice", "")
                        output_data.append([mpn,
                                            part_manufacturer,
                                            seller_id,
                                            seller_name,
                                            stock,
                                            quantity,
                                            converted_price])
    return output_data

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5001)
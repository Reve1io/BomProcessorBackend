import os
import asyncio
from flask import Flask, request, jsonify
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from flask_cors import CORS
from services.nexar_service import process_all_mpn

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
        nexar_data = loop.run_until_complete(process_all_mpn(mpn_list, mode, logger=app.logger))
    except Exception as e:
        app.logger.error(f"Ошибка при обработке: {str(e)}", exc_info=True)
        return jsonify({"status": "error", "message": str(e) or "Неизвестная ошибка при обработке"}), 500

    return jsonify({"data": nexar_data})

if __name__ == '__main__':
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", 5002))
    app.run(host=host, port=port)
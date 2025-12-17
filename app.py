import os
import asyncio
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from services.nexar_service import process_all_mpn
from flask_cors import CORS
from flask import Flask, request, jsonify
from redis_config import task_queue, redis_conn
from rq.job import Job
from services.nexar_service import run_nexar_task

load_dotenv()

app = Flask(__name__)

environment = os.getenv("ENVIRONMENT", "prod")
client_app = os.getenv("CLIENT_APP", "http://localhost:5173")
client_tellur = os.getenv("CLIENT_TELLUR")

if environment == "local":
    print("Running in LOCAL mode → enabling Flask CORS")
    CORS(app, resources={
        r"/api/*": {
            "origins": [client_app, client_tellur],
            "methods": ["POST", "GET", "OPTIONS"],
            "allow_headers": ["Content-Type", "Authorization"],
            "supports_credentials": True
        }
    })
else:
    print("Running in PROD mode → CORS handled by NGINX")

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

#Старая версия эндпоинта
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

app = Flask(__name__)

#Новый версия эндпоинта
@app.route('/api/v1/process', methods=['POST'])
def submit_task():
    data = request.get_json(force=True)
    app.logger.info(f"Получен запрос на /api/v1/process: {data}")

    mapping = data.get("mapping", {})
    mode = data.get("mode", "full")
    rows = data.get("data", [])
    q_param = data.get("q")

    if not rows and not q_param:
        return jsonify({"error": "Нет данных или параметра 'q'"}), 400

    mpn_list = []

    if rows:
        keys = list(mapping.keys())
        values = list(mapping.values())

        try:
            part_index = int(keys[values.index("partNumber")])
        except (ValueError, Exception):
            return jsonify({"error": "Не удалось распознать индекс partNumber в mapping"}), 400

        quantity_index = None
        if "quantity" in values:
            try:
                quantity_index = int(keys[values.index("quantity")])
            except Exception:
                quantity_index = None

        header_keywords = ["mpn", "partnumber", "количество", "quantity", "manufacturer", "производитель"]
        start_index = 1 if any(str(cell).lower() in header_keywords for cell in rows[0]) else 0

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

    elif q_param:
        mpn_list.append({"mpn": q_param, "quantity": None})

    if not mpn_list:
        return jsonify({"error": "Список MPN пуст после обработки данных"}), 400

    app.logger.info(f"Сформирован список для очереди: {len(mpn_list)} позиций")

    try:
        job = task_queue.enqueue(
            run_nexar_task,
            mpn_list,
            mode,
            job_timeout='2h'
        )

        return jsonify({
            "status": "PENDING",
            "message": "Задача успешно создана",
            "task_id": job.id,
            "check_url": f"/api/v1/status/{job.id}"
        }), 202

    except Exception as e:
        app.logger.error(f"Ошибка при добавлении в Redis: {str(e)}")
        return jsonify({"error": "Сервис временно недоступен (Redis error)"}), 503


# Эндпоинт для проверки статуса
@app.route('/api/v1/status/<task_id>', methods=['GET'])
def get_task_status(task_id):
    try:
        job = Job.fetch(task_id, connection=redis_conn)
    except Exception:
        return jsonify({"status": "NOT_FOUND"}), 404

    if job.is_finished:
        return jsonify({
            "status": "COMPLETED",
            "data": job.result.get("result") if isinstance(job.result, dict) else job.result
        }), 200

    elif job.is_failed:
        return jsonify({"status": "FAILED", "error": str(job.exc_info)}), 500

    return jsonify({"status": job.get_status()}), 200

if __name__ == '__main__':
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", 5003))
    app.run(host=host, port=port)
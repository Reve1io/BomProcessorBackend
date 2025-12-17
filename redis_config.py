import os
import redis
from rq import Queue
from dotenv import load_dotenv

load_dotenv()

# --- Конфигурация Redis (используем переменные окружения) ---

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 1))

print(f"Попытка подключения к Redis: {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")

try:
    # Инициализация клиента Redis.
    redis_conn = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        socket_timeout=5,  # Таймаут для подключения, чтобы не зависать
    )

    # Проверка соединения (вызывает ошибку, если нет записи)
    # Попытка записи, чтобы убедиться, что это Master, а не ReadOnly Replica
    # Мы используем команду SET, которая гарантирует ошибку, если это реплика.
    # Мы ставим временный ключ, чтобы не засорять базу.
    redis_conn.set('rq:test_write', '1', ex=1)
    print("Соединение с Redis успешно установлено (Master/Writable).")

except redis.exceptions.ConnectionError as e:
    print(f"Ошибка подключения к Redis: {e}")
    raise ConnectionError(f"Не удалось подключиться к Redis по адресу {REDIS_HOST}:{REDIS_PORT}")
except redis.exceptions.ReadOnlyError as e:
    print(f"Ошибка: Redis-сервер {REDIS_HOST} является Read-Only Replica. Невозможно запустить Worker.")
    raise PermissionError("Подключение к ReadOnly Redis. Пожалуйста, проверьте адрес Master-сервера.")

# Инициализация очереди RQ.
task_queue = Queue('search_mpn', connection=redis_conn)
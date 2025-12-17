import sys
from rq import Worker, SimpleWorker
from redis_config import redis_conn

if __name__ == '__main__':
    queues = ['search_mpn']

    # Проверяем операционную систему
    if sys.platform == "win32":
        print("=== Запуск в режиме Windows (SimpleWorker) ===")
        worker_class = SimpleWorker
    else:
        print("=== Запуск в режиме Linux/Unix (Standard Worker) ===")
        worker_class = Worker

    worker = worker_class(queues, connection=redis_conn)
    worker.work()
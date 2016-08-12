from celery import Celery

BROKER_URL = 'redis://localhost:6379/0'
CELERY_RESULT_BACKEND = 'redis://localhost:6379/1'

app = Celery(
    'lumberjack',
    broker=BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
    include=['lumberjack.tasks']
)

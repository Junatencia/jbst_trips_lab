# celery_app.py
from celery import Celery
import os

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
celery_app = Celery(
    "trips",
    broker=REDIS_URL,
    backend=REDIS_URL,
)

# Optional: settings
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    task_track_started=True,
)

from app.tasks.worker import celery

@celery.task(name="app.tasks.system_monitor_tasks.heartbeat.ping")
def ping(name: str):
    return name

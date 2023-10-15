from app.tasks.worker import MetabolightsTask, celery
import app
from billiard import current_process
@celery.task(bind=True, base=MetabolightsTask, name="app.tasks.system_monitor_tasks.heartbeat.ping")
def ping(self, input: str):
    process = current_process()
    return {"reply_for": input, "worker_version": app.__api_version__, "name": self.request.hostname, "celery_name": process._target.initargs[1]}

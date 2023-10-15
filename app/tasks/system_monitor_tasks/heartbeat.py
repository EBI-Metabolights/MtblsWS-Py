from app.tasks.worker import MetabolightsTask, celery
import app
from billiard import current_process
@celery.task(bind=True, base=MetabolightsTask, name="app.tasks.system_monitor_tasks.heartbeat.ping")
def ping(self, input: str):
    process = current_process()
    return {"input": input, "version": app.__api_version__, "requester": self.request.hostname, "responder": process._target.initargs[1]}

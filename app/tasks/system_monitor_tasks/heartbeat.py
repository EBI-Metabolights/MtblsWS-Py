from app.tasks.worker import MetabolightsTask, celery
import app


@celery.task(
    bind=True,
    base=MetabolightsTask,
    name="app.tasks.system_monitor_tasks.heartbeat.ping",
)
def ping(self, input_value: str):
    return {
        "reply_for": input_value,
        "worker_version": app.__api_version__,
        "worker_name": self.request.hostname,
    }

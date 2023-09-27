import logging
from typing import Any, Dict, List, Union

from app.config import get_settings
from app.tasks.system_monitor_tasks.datamover_worker_monitor import \
    maintain_datamover_workers
from app.tasks.system_monitor_tasks.vm_worker_monitor import maintain_vm_workers
from app.tasks.worker import MetabolightsTask, celery

logger = logging.getLogger("beat")


@celery.task(
    bind=True,
    base=MetabolightsTask,
    name="app.tasks.system_monitor_tasks.worker_maintenance.check_all_workers",
)
def check_all_workers(self):

    registered_workers = celery.control.inspect().stats()
    datamover_results: Dict[str, List[str]] = check_datamover_workers(registered_workers=registered_workers)
    vm_results = check_vm_workers(None, registered_workers=registered_workers)
    all_results = vm_results.update(datamover_results)
    return all_results


@celery.task(
    bind=True,
    base=MetabolightsTask,
    name="app.tasks.system_monitor_tasks.worker_maintenance.check_vm_workers",
)
def check_vm_workers(
    self, hostnames: Union[str, List[str]]=None, registered_workers: Dict[str, Any] = None
) -> Dict[str, str]:
    worker_settings = get_settings().workers.vm_workers
    if not hostnames:
        hostnames = [x.hostname for x in worker_settings.hosts]
    else:
        hostnames = hostnames.split(",") if isinstance(hostnames, str) else hostnames
      
    results = {}
    if not hostnames:
        return results
    hosts = worker_settings.hosts
    if not registered_workers:
        registered_workers = celery.control.inspect().stats()
    for host in hosts:
        if host.hostname in hostnames:
            result = maintain_vm_workers(host, registered_workers=registered_workers)
            results.update(result)
    return results


@celery.task(
    bind=True,
    base=MetabolightsTask,
    name="app.tasks.system_monitor_tasks.worker_maintenance.check_datamover_workers",
)
def check_datamover_workers(
    self, registered_workers: Dict[str, Any] = None
) -> Dict[str, str]:
    return maintain_datamover_workers(registered_workers=registered_workers)


if __name__ == "__main__":
    # check_additional_vm_workers()
    result = check_vm_workers()
    # result = check_additional_vm_workers()
    print(result)

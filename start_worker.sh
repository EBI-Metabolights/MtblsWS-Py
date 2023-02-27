#!/bin/bash

eval "$(conda shell.bash hook)"
conda activate python38-mtblsws-celery
# kill all processes, just in case
# killall gunicorn

source .env
export $(cat .env | grep -v '#' | xargs)

python -m celery -A app.tasks.worker:celery worker --loglevel info -Q mtbls-tasks --autoscale 5,20 --logfile logs/celery_worker_$(hostname).log --detach
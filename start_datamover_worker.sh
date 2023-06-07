#!/bin/bash

HOST=$(hostname)
echo "Starting Celery datamover worker on host $HOST"
WORKER_NAME="$1"

if [ -z "$WORKER_NAME" ]; then
    WORKER_NAME="datamover_worker_1"
fi

QUEUE_NAME="$2"

if [ -z "$QUEUE_NAME" ]; then
    QUEUE_NAME="datamover-tasks"
fi

APPDIR="/app-root"

APP_LOG_DIR=$APPDIR/logs
PYTHONPATH="$APPDIR:$PYTHONPATH"

source $APPDIR/.env 
export $(cat $APPDIR/.env | grep -v '#' | xargs)

echo "Python version:" $(python3 --version)
echo "Host name: $HOST"

cd $APPDIR
python3 -m celery -A app.tasks.worker:celery worker --loglevel debug -Q $QUEUE_NAME --autoscale 2,20 --logfile $APP_LOG_DIR/celery_datamover_worker.log -n ${WORKER_NAME}@%h
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

LIVENESS_QUEUE_NAME="$3"

APPDIR="/app-root"

if [ -z "$LOGS_PATH" ]; then
    LOGS_PATH=$APPDIR/logs
fi
export CONFIG_FILE_PATH="$APPDIR/config.yaml"
export SECRETS_PATH="$APPDIR/.secrets"

export PYTHONPATH="$APPDIR:$PYTHONPATH"

echo "Python version:" $(python3 --version)
echo "Host name: $HOST"

cd $APPDIR
python3 -m celery -A app.tasks.worker:celery worker --loglevel info -Q "$QUEUE_NAME,$LIVENESS_QUEUE_NAME" --autoscale 2,5 --logfile $LOGS_PATH/celery_datamover_worker_${WORKER_NAME}.log -n ${WORKER_NAME}@%h
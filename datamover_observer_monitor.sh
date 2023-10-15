#!/bin/bash

HOST=$(hostname)
echo "Starting Celery datamover worker proxy on $HOST"
WORKER_NAME="$1"

if [ -z "$WORKER_NAME" ]; then
    WORKER_NAME="datamover_worker_1"
fi

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
python3 app/tasks/datamover_monitor.py "$WORKER_NAME"
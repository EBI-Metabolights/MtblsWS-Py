#!/bin/bash
SERVER_PORT="$1"

if [ -z "$SERVER_PORT" ]; then
    echo "SERVER PORT parameter is not defined. execute with port number"
    exit 1
fi

CONDA_ENVIRONMENT="$2"
if [ -z "$CONDA_ENVIRONMENT" ]; then
    CONDA_ENVIRONMENT="python38-MtblsWS"
    echo "CONDA_ENVIRONMENT is not defined. $CONDA_ENVIRONMENT conda environment will be used."
fi

if [ -z "$CONFIG_FILE_PATH" ]; then
    CONFIG_FILE_PATH="$APPDIR/config.yaml"
fi

if [ -z "$SECRETS_PATH" ]; then
    SECRETS_PATH="$APPDIR/.secrets"
fi

HOST=$(hostname)
echo $HOST

APPDIR=$(pwd -P)
LOG_PATH=$APPDIR/logs

cd $APPDIR
eval "$(conda shell.bash hook)"
conda activate $CONDA_ENVIRONMENT


PROCESS_ID=$(ps -aux | grep "$LOG_PATH/celery_monitor_worker_${HOST}_${SERVER_PORT}.log" |  grep -v "grep" | awk '{ print $2 }' | tr '\n' ' ')

if [ -z "$PROCESS_ID" ]; then
    echo "CELERY MONITOR WORKER will be started"
    python3 -m celery -A app.tasks.worker:celery worker -Q monitor-tasks  --logfile $LOG_PATH/celery_monitor_worker_${HOST}_${SERVER_PORT}.log --loglevel info -n monitor_worker@%h --concurrency 1 --detach
    if [ $? -eq 0 ]; then
        echo "Celery monitor worker is up."
    else
        echo "Celery monitor worker start task is failed."
        exit 1
    fi
else
    echo "CELERY MONITOR WORKER is running. PROCESS_ID: ${PROCESS_ID}"
fi


PROCESS_ID=$(ps -aux | grep "$LOG_PATH/celery_beat_${HOST}_${SERVER_PORT}.log" | grep -v "grep" | awk '{ print $2 }' |  tr '\n' ' ')
if [ -z "$PROCESS_ID" ]; then
    echo "CELERY BEAT will be started"
    python3 -m celery -A app.tasks.worker:celery beat --logfile $LOG_PATH/celery_beat_${HOST}_${SERVER_PORT}.log --loglevel info --detach
    if [ $? -eq 0 ]; then
        echo "Celery beat worker is up."
    else
        echo "Celery beat start task is failed."
        exit 1
    fi
else
    echo "CELERY BEAT is running. PROCESS_ID: ${PROCESS_ID}"
fi
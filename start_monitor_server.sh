#!/bin/bash
SERVER_PORT="$1"

if [ -z "$SERVER_PORT" ]; then
    echo "SERVER PORT parameter is not defined. execute with port number"
    exit 1
fi

if [ -z "$CONFIG_FILE_PATH" ]; then
    CONFIG_FILE_PATH="$APPDIR/config.yaml"
fi

if [ -z "$SECRETS_PATH" ]; then
    SECRETS_PATH="$APPDIR/.secrets"
fi

HOST=$(hostname)
echo $HOST

APPDIR=$PWD
LOG_PATH=$APPDIR/logs

cd $APPDIR
eval "$(conda shell.bash hook)"
conda activate python38-MtblsWS


PROCESS_ID=$(ps -aux | grep "$LOG_PATH/celery_monitor_worker_${HOST}_${SERVER_PORT}.log" | awk '{ print $2 }' | head -n -1 | tr '\n' ' ')

if [ -z "$PROCESS_ID" ]; then
    echo "CELERY MONITOR WORKER will be started"
    python3 -m celery -A app.tasks.worker:celery worker -Q monitor-tasks  --logfile $LOG_PATH/celery_monitor_worker_${HOST}.log --loglevel info -n monitor_worker@%h --autoscale 1,2 --detach
else
    echo "CELERY MONITOR WORKER is running. PROCESS_ID: ${PROCESS_ID}"
fi


PROCESS_ID=$(ps -aux | grep "$LOG_PATH/celery_beat_${HOST}_${SERVER_PORT}.log" | awk '{ print $2 }' |  head -n -1 | tr '\n' ' ')
if [ -z "$PROCESS_ID" ]; then
    echo "CELERY BEAT will be started"
    python3 -m celery -A app.tasks.worker:celery beat --logfile $LOG_PATH/celery_beat_${HOST}.log --loglevel info --detach
else
    echo "CELERY BEAT is running. PROCESS_ID: ${PROCESS_ID}"
fi
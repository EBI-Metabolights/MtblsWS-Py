#!/bin/bash
SERVER_PORT="$1"

if [ -z "$SERVER_PORT" ]; then
    echo "SERVER PORT parameter is not defined. execute with port number"
    exit 1
fi

HOST=$(hostname)
APPDIR=$PWD
LOG_PATH=$APPDIR/logs

PROCESS_ID=$(ps -ef | grep "$LOG_PATH/gunicorn_${HOST}_${SERVER_PORT}" | awk '{ print $2 }' | head -n -1 | tr '\n' ' ')

if [ -z "$PROCESS_ID" ]; then
    EXISTING_PROCESS=$(netstat -plant 2>/dev/null | grep $SERVER_PORT | awk '{print $7}' | tr "/" " ")
    echo $EXISTING_PROCESS
    if [ -z "$EXISTING_PROCESS" ]; then
        echo "NO GUNICORN SERVER on port $SERVER_PORT"
    else
        echo "An application is already running on port $SERVER_PORT. It may be gunicorn server. Current process id and process name: $EXISTING_PROCESS"
    fi
else
    echo "GUNICORN SERVER PROCESS_ID: ${PROCESS_ID}"
fi

PROCESS_ID=$(ps -aux | grep "$LOG_PATH/celery_monitor_worker_${HOST}_${SERVER_PORT}.log" | awk '{ print $2 }' | head -n -1 | tr '\n' ' ')

if [ -z "$PROCESS_ID" ]; then
    echo "NO CELERY MONITOR WORKER"
else
    echo "CELERY MONITOR WORKER PROCESS_ID: ${PROCESS_ID}"
fi

PROCESS_ID=$(ps -aux | grep "$LOG_PATH/celery_beat_${HOST}_${SERVER_PORT}.log" | awk '{ print $2 }' |  head -n -1 | tr '\n' ' ')

if [ -z "$PROCESS_ID" ]; then
    echo "NO CELERY BEAT"
else
    echo "CELERY BEAT PROCESS_ID: ${PROCESS_ID}"
fi

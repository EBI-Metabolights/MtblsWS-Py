#!/bin/bash
SERVER_PORT="$1"

if [ -z "$SERVER_PORT" ]; then
    echo "SERVER PORT parameter is not defined. execute with port number"
    exit 1
fi

HOST=$(hostname)
APPDIR=$PWD
LOG_PATH=$APPDIR/logs

SEARCH="gunicorn -b 0.0.0.0:$SERVER_PORT --access-logfile $APPDIR/logs/gunicorn_$HOST --error-logfile "
PROCESS_ID=$(ps -ef | grep "$SEARCH" | awk '{ print $2 }' | head -n -1 | tr '\n' ' ')

if [ -z "$PROCESS_ID" ]; then
    echo "NO GUNICORN SERVER"
else
    echo "GUNICORN SERVER PROCESS_ID: ${PROCESS_ID}"
fi

PROCESS_ID=$(ps -aux | grep ":celery worker -Q monitor-tasks --logfile $LOG_PATH/celery_monitor_worker_${HOST}.log --loglevel info -n monitor_worker@" | awk '{ print $2 }' | head -n -1 | tr '\n' ' ')

if [ -z "$PROCESS_ID" ]; then
    echo "NO CELERY MONITOR WORKER"
else
    echo "CELERY MONITOR WORKER PROCESS_ID: ${PROCESS_ID}"
fi

PROCESS_ID=$(ps -aux | grep ":celery beat --logfile $LOG_PATH/celery_beat_${HOST}.log" | awk '{ print $2 }' |  head -n -1 | tr '\n' ' ')

if [ -z "$PROCESS_ID" ]; then
    echo "NO CELERY BEAT"
else
    echo "CELERY BEAT PROCESS_ID: ${PROCESS_ID}"
fi

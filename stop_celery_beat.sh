#!/bin/bash
SERVER_PORT="$1"

if [ -z "$SERVER_PORT" ]; then
    echo "SERVER PORT parameter is not defined. execute with port number"
    exit 1
fi
HOST=$(hostname)
APPDIR=$(pwd -P)
LOG_PATH=$APPDIR/logs

PROCESS_ID=$(ps -ef | grep "$LOG_PATH/celery_beat_${HOST}_${SERVER_PORT}.log" |  grep -v "grep" | awk '{ print $2 }' | tr '\n' ' ')

if [ -z "$PROCESS_ID" ]; then
    echo "NO CELERY BEAT"
else
    echo "CELERY BEAT PROCESS_ID: ${PROCESS_ID} will be killed"
    kill -9 $PROCESS_ID
fi

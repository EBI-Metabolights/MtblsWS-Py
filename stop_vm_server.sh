#!/bin/bash
SERVER_PORT="$1"

if [ -z "$SERVER_PORT" ]; then
    echo "SERVER PORT parameter is not defined. execute with port number"
    exit 1
fi
HOST=$(hostname)
APPDIR=$PWD
LOG_PATH=$APPDIR/logs
PROCESS_ID=$(ps -ef | grep "gunicorn -b 0.0.0.0:$SERVER_PORT --access-logfile $APPDIR/logs/gunicorn_$HOST --error-logfile" | awk '{ print $2 }' | head -n -1 | tr '\n' ' ')

if [ -z "$PROCESS_ID" ]; then
    echo "NO GUNICORN SERVER"
else
    echo "GUNICORN SERVER PROCESS_ID: ${PROCESS_ID} will be killed"
    kill -9 $PROCESS_ID
fi

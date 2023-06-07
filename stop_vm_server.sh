#!/bin/bash
SERVER_PORT=5001
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

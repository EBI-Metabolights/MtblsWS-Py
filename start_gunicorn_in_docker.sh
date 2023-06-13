#!/bin/bash

HOST=$(hostname)
echo "Starting Green Unicorn server on host $HOST "

APPDIR=$PWD
if [ -z "$LOGS_PATH" ]; then
    LOGS_PATH=$APPDIR/logs
fi

if [ -z "$CONFIG_FILE_PATH" ]; then
    CONFIG_FILE_PATH="$APPDIR/config.yaml"
fi

if [ -z "$SECRETS_PATH" ]; then
    SECRETS_PATH="$APPDIR/.secrets"
fi

export PYTHONPATH="$APPDIR:$PYTHONPATH"

LOG=$LOGS_PATH/gunicorn_$HOST

#launch WS
gunicorn --workers 3 --threads 2 -b 0.0.0.0:5000 --worker-class gevent --pid $LOGS_PATH/app_$HOST.pid --preload wsapp:app

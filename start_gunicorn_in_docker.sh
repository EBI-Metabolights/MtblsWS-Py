#!/bin/bash
SERVER_PORT=7007
APPDIR="/app-root"

if [ -z "$CONFIG_FILE_PATH" ]; then
    CONFIG_FILE_PATH="$APPDIR/config.yaml"
fi

if [ -z "$SECRETS_PATH" ]; then
    SECRETS_PATH="$APPDIR/.secrets"
fi

HOST=$(hostname)
echo $HOST

LOG_PATH=$APPDIR/logs

cd $APPDIR

gunicorn -b 0.0.0.0:$SERVER_PORT --worker-class gevent --preload wsapp:application --workers 1 --threads 3 --pid $LOG_PATH/app_${HOST}_${SERVER_PORT}.pid  --log-level info
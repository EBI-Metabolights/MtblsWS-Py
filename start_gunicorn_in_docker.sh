#!/bin/bash
SERVER_PORT="$1"
NUMBER_OF_WORKERS="$2"
if [ -z "$SERVER_PORT" ]; then
    SERVER_PORT="7007"
    echo "DEFAULT SERVER PORT will be used $SERVER_PORT"
fi
if [ -z "$NUMBER_OF_WORKERS" ]; then
    NUMBER_OF_WORKERS=3
fi

APPDIR="/app-root"
echo "SERVER PORT is $SERVER_PORT"
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

echo Command: gunicorn -b 0.0.0.0:$SERVER_PORT --workers $NUMBER_OF_WORKERS  wsapp:application --forwarded-allow-ips \"*\"  --pid $LOG_PATH/app_${HOST}_${SERVER_PORT}.pid  --log-level info

gunicorn -b 0.0.0.0:$SERVER_PORT --workers $NUMBER_OF_WORKERS  wsapp:application --forwarded-allow-ips "*"  --pid $LOG_PATH/app_${HOST}_${SERVER_PORT}.pid  --log-level info
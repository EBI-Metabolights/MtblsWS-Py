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

HOST=$(hostname)
APPDIR=$(pwd -P)
LOG_PATH=$APPDIR/logs

PROCESS_ID=$(ps -aux | grep "$LOG_PATH/celery_beat_${HOST}_${SERVER_PORT}.log" | grep -v "grep" | awk '{ print $2 }' | tr '\n' ' ')

if [ -z "$PROCESS_ID" ]; then
    echo "NO CELERY BEAT"
else
    echo "CELERY BEAT PROCESS_ID: ${PROCESS_ID} will be killed"
    kill -9 $PROCESS_ID
fi

cd $APPDIR

if [ -z "$CONFIG_FILE_PATH" ]; then
    CONFIG_FILE_PATH="$APPDIR/config.yaml"
fi

if [ -z "$SECRETS_PATH" ]; then
    SECRETS_PATH="$APPDIR/.secrets"
fi

eval "$(conda shell.bash hook)"
conda activate $CONDA_ENVIRONMENT

source .env
export $(cat .env | grep -v '#' | xargs)


echo "Shutdown signal will be sent to all workers"
export C_FAKEFORK=1
celery -A app.tasks.worker:celery control shutdown
if [ $? -eq 0 ]; then
    echo "Shutdown signal was sent to all workers"
else
    echo "Shutdown signal task is failed."
    exit 1
fi

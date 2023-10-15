#!/bin/bash
# SERVER_PORT="$1"

# if [ -z "$SERVER_PORT" ]; then
#     echo "SERVER PORT parameter is not defined. execute with port number"
#     exit 1
# fi
# WORKER_NAME="$2"

# if [ -z "$WORKER_NAME" ]; then
#     echo "WORKER_NAME parameter is not defined. execute with port number and worker name"
#     exit 1
# fi


HOST=$(hostname)
echo $HOST

APPDIR=$(pwd -P)
LOG_PATH=$APPDIR/logs

cd $APPDIR
eval "$(conda shell.bash hook)"
conda activate $CONDA_ENVIRONMENT

celery -A app.tasks.worker:celery inspect active
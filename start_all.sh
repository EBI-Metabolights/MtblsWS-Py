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
echo $HOST
echo "Host $HOST approved, starting Green Unicorn server"

APPDIR=$(pwd -P)

if [ -z "$SERVER_PORT" ]; then
    echo "SERVER PORT parameter is not defined. execute with port number"
    exit 1
fi

LOG_PATH=$APPDIR/logs
if [ -z "$CONFIG_FILE_PATH" ]; then
    CONFIG_FILE_PATH="$APPDIR/config.yaml"
fi

if [ -z "$SECRETS_PATH" ]; then
    SECRETS_PATH="$APPDIR/.secrets"
fi

PYTHONPATH=$APPDIR

cd $APPDIR

eval "$(conda shell.bash hook)"
conda activate $CONDA_ENVIRONMENT


PROCESS_ID=$(ps -aux | grep "$LOG_PATH/celery_monitor_worker_${HOST}_${SERVER_PORT}.log" |  grep -v "grep" | awk '{ print $2 }' | tr '\n' ' ')

if [ -z "$PROCESS_ID" ]; then
    echo "CELERY MONITOR WORKER will be started"
    python3 -m celery -A app.tasks.worker:celery worker -Q monitor-tasks  --logfile $LOG_PATH/celery_monitor_worker_${HOST}_${SERVER_PORT}.log --loglevel info -n monitor_worker@%h --concurrency 1 --detach
    if [ $? -eq 0 ]; then
        echo "Celery monitor worker is up."
    else
        echo "Celery monitor worker start task is failed."
        exit 1
    fi
else
    echo "CELERY MONITOR WORKER is running. PROCESS_ID: ${PROCESS_ID}"
fi


PROCESS_ID=$(ps -aux | grep "$LOG_PATH/celery_beat_${HOST}_${SERVER_PORT}.log" | grep -v "grep" | awk '{ print $2 }' |  tr '\n' ' ')
if [ -z "$PROCESS_ID" ]; then
    echo "CELERY BEAT will be started"
    python3 -m celery -A app.tasks.worker:celery beat --logfile $LOG_PATH/celery_beat_${HOST}_${SERVER_PORT}.log --loglevel info --detach
    if [ $? -eq 0 ]; then
        echo "Celery beat worker is up."
    else
        echo "Celery beat start task is failed."
        exit 1
    fi
else
    echo "CELERY BEAT is running. PROCESS_ID: ${PROCESS_ID}"
fi


PROCESS_ID=$(ps -ef | grep "$LOG_PATH/gunicorn_${HOST}_${SERVER_PORT}" | grep -v "grep"  | awk '{ print $2 }'  | tr '\n' ' ')

if [ -z "$PROCESS_ID" ]; then
    EXISTING_PROCESS=$(netstat -plant 2>/dev/null | grep $SERVER_PORT | awk '{print $7}') | tr "/" " "
    if [ -z "$EXISTING_PROCESS" ]; then
        echo "GUNICORN will be started"
        gunicorn -b 0.0.0.0:$SERVER_PORT --access-logfile $LOG_PATH/gunicorn_${HOST}_${SERVER_PORT}.log --error-logfile $LOG_PATH/gunicorn_${HOST}_${SERVER_PORT}.log --preload wsapp:application --workers 3 --threads 2 --pid ./app_${HOST}_${SERVER_PORT}.pid  --log-level info --capture-output --daemon  > $LOG_PATH/gunicorn_${HOST}_${SERVER_PORT} 3>&1 & echo $! > app_${HOST}_${SERVER_PORT}.pid
        if [ $? -eq 0 ]; then
            echo "Gunicorn is up."
        else
            echo "Gunicorn start task is failed."
            exit 1
        fi
    else
        echo "!!!WARNING: An application is already running on port $SERVER_PORT. Kill this process before starting server. Current process id and process name $EXISTING_PROCESS"
    fi
else
    echo "!!!WARNING: GUNICORN is already running. PROCESS_ID: ${PROCESS_ID}. Kill the process before starting server"
fi

#launch WS

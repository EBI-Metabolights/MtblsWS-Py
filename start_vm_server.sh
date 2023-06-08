#!/bin/bash
. ../../functions.inc
HOST=$(hostname)
echo $HOST
check_host .
echo "Host $HOST approved, starting Green Unicorn server"

APPDIR=$PWD
LOG_PATH=$APPDIR/logs

mkdir -p $APPDIR/logs


PYTHONPATH=$APPDIR

LOG=$APPDIR/logs/gunicorn_$HOST
cd $APPDIR
SERVER_PORT=5001
# activate Python virtual environment
#source $VENVDIR/bin/activate
eval "$(conda shell.bash hook)"
conda activate python38-MtblsWS
# kill all processes, just in case
# killall gunicorn

source .env
#export $(cat .env | xargs)
export $(cat .env | grep -v '#' | xargs)


PROCESS_ID=$(ps -aux | grep ":celery worker -Q monitor-tasks --logfile $LOG_PATH/celery_monitor_worker_${HOST}.log --loglevel info -n monitor_worker@" | awk '{ print $2 }' | head -n -1 | tr '\n' ' ')

if [ -z "$PROCESS_ID" ]; then
    echo "CELERY MONITOR WORKER will be started"
    python3 -m celery -A app.tasks.worker:celery worker -Q monitor-tasks  --logfile $LOG_PATH/celery_monitor_worker_${HOST}.log --loglevel info -n monitor_worker@%h --autoscale 1,2 --detach
else
    echo "CELERY MONITOR WORKER is running. PROCESS_ID: ${PROCESS_ID}"
fi


PROCESS_ID=$(ps -aux | grep ":celery beat --logfile $LOG_PATH/celery_beat_${HOST}.log" | awk '{ print $2 }' |  head -n -1 | tr '\n' ' ')
if [ -z "$PROCESS_ID" ]; then
    echo "CELERY BEAT will be started"
    python3 -m celery -A app.tasks.worker:celery beat --logfile $LOG_PATH/celery_beat_${HOST}.log --loglevel info --detach
else
    echo "CELERY BEAT is running. PROCESS_ID: ${PROCESS_ID}"
fi


SEARCH="gunicorn -b 0.0.0.0:$SERVER_PORT --access-logfile $APPDIR/logs/gunicorn_$HOST --error-logfile "
PROCESS_ID=$(ps -ef | grep "$SEARCH" | awk '{ print $2 }' | head -n -1 | tr '\n' ' ')

if [ -z "$PROCESS_ID" ]; then
    EXISTING_PROCESS=$(netstat -plant 2>/dev/null | grep $SERVER_PORT | awk '{print $7}') | tr "/" " "
    if [ -z "$EXISTING_PROCESS" ]; then
        echo "GUNICORN will be started"
        gunicorn -b 0.0.0.0:$SERVER_PORT --access-logfile $APPDIR/logs/gunicorn_$HOST --error-logfile $APPDIR/logs/gunicorn_$HOST  --worker-class gevent --preload wsapp:app --workers 3 --threads 2 --pid ./app_${HOST}.pid  --log-level info --capture-output --daemon  > $LOG 3>&1 & echo $! > app_$HOST.pid
    else
        echo "!!!WARNING: An application is already running on port $SERVER_PORT. Kill this process before starting server. Current process id and process name $EXISTING_PROCESS"
    fi
else
    echo "!!!WARNING: GUNICORN is already running. PROCESS_ID: ${PROCESS_ID}. Kill the process before starting server"
fi

#launch WS

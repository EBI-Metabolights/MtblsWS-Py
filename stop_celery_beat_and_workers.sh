#!/bin/bash
SERVER_PORT=5001
HOST=$(hostname)
APPDIR=$PWD
LOG_PATH=$APPDIR/logs

PROCESS_ID=$(ps -ef | grep "python3 -m celery -A app.tasks.worker:celery beat --logfile $LOG_PATH/celery_beat_${HOST}.log" | awk '{ print $2 }' | head -n -1 | tr '\n' ' ')

if [ -z "$PROCESS_ID" ]; then
    echo "NO CELERY BEAT"
else
    echo "CELERY BEAT PROCESS_ID: ${PROCESS_ID} will be killed"
    kill -9 $PROCESS_ID
fi

cd $APPDIR

eval "$(conda shell.bash hook)"
conda activate python38-MtblsWS

source .env
export $(cat .env | grep -v '#' | xargs)
echo "Shutdown signal will be sent to all workers"
celery -A app.tasks.worker:celery control shutdown -t 10
echo "Shutdown signal was sent to all workers"
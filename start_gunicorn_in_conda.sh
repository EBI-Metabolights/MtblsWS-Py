#!/bin/bash
. ~/metabolights/scripts/functions.inc
HOST=$(hostname)
echo $HOST
check_host .
echo "Host $HOST approved, starting Green Unicorn server"

APPDIR=$PWD

PATH=~/opt/sqlite/bin:$PATH
LD_LIBRARY_PATH=~/opt/sqlite/lib:$LD_LIBRARY_PATH
LD_RUN_PATH=~/opt/sqlite/lib:$LD_RUN_PATH

mkdir -p $APPDIR/logs
mkdir -p $APPDIR/instance

PYTHONPATH=$APPDIR
LOG=$APPDIR/logs/gunicorn_$HOST
cd $APPDIR

eval "$(conda shell.bash hook)"
conda activate python38-MtblsWS
# kill all processes, just in case
# killall gunicorn

source .env
#export $(cat .env | xargs)
export $(cat .env | grep -v '#' | xargs)
#launch WS
gunicorn --workers 3 --threads 2 -b 0.0.0.0:5000 --worker-class gevent --pid ./app_$HOST.pid --preload wsapp:app --access-logfile $LOG --error-logfile $LOG --log-level info --capture-output --daemon > $LOG 3>&1 & echo $! > app_$HOST.pid

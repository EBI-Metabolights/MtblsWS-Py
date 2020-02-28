#!/bin/bash
. ../../functions.inc
HOST=$(hostname)
echo $HOST
check_host .
echo "Host $HOST approved, starting Green Unicorn server"

APPDIR=$PWD
VENVDIR=$APPDIR/venv368

#PATH=$~/opt/sqlite/bin:$PATH
#LD_LIBRARY_PATH=$~/opt/sqlite/lib:$LD_LIBRARY_PATH
#LD_RUN_PATH=$~/opt/sqlite/lib:$LD_RUN_PATH

PYTHONPATH=$APPDIR
LOG=$APPDIR/logs/gunicorn_$HOST
cd $APPDIR

# activate Python virtual environment
source $VENVDIR/bin/activate

# kill all processes, just in case
killall gunicorn

#launch WS
gunicorn --workers 3 --threads 2 -b 0.0.0.0:5005 --worker-class gevent --pid ./app_$HOST.pid --preload wsapp:app --access-logfile $LOG --error-logfile $LOG --log-level info --capture-output --daemon > $LOG 2>&1 & echo $! > app_$HOST.pid

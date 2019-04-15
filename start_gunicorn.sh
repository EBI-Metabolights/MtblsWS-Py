#!/bin/bash
. ../../functions.inc
HOST=`hostname`
echo $HOST
check_host .
echo "Host $HOST approved, starting Green Unicorn server"

APPDIR=$PWD
VENVDIR=$APPDIR/venv363

PYTHONPATH=$APPDIR
cd $APPDIR

# activate RH collections for Python 3.6.3
source scl_source enable rh-python36

# activate Python virtual environment and launch WS
source $VENVDIR/bin/activate

# kill all processes, just in case
killall gunicorn
gunicorn --workers 3 --threads 2 -b 0.0.0.0:5005 --worker-class gevent --pid ./app_$(uname -n).pid --preload wsapp:app --access-logfile ./logs/gunicorn_$(uname -n).log --error-logfile ./logs/gunicorn_$(uname -n).log --log-level info --capture-output --daemon > ./logs/logs_$(uname -n).txt 2>&1 & echo $! > app_$(uname -n).pid

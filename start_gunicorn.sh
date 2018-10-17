#!/bin/bash

APPDIR=$PWD
VENVDIR=$APPDIR/venv351

PYTHONPATH=$APPDIR

cd $APPDIR

# activate RH collections for Python 3.5.1
source scl_source enable rh-python35

# activate Python virtual environment and launch WS
source $VENVDIR/bin/activate

gunicorn --workers 3 --threads 2 -b 0.0.0.0:5005 -p ./app.pid --preload wsapp:app --access-logfile ./logs/gunicorn.log --error-logfile ./logs/gunicorn.log --log-level info --capture-output --daemon > logs.txt 2>&1 & echo $! > app.pid

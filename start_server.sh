#!/bin/bash

APPDIR=$PWD
VENVDIR=$APPDIR/venv351

PYTHONPATH=$APPDIR

cd $APPDIR

# activate RH collections for Python 3.5.1
source scl_source enable rh-python35

# activate Python virtual environment and launch WS
source $VENVDIR/bin/activate
python $APPDIR/wsapp.py > logs.txt 2>&1 & echo $! > app.pid

#!/bin/bash

APPDIR=$PWD
VENVDIR=$APPDIR/venv

PYTHONPATH=$APPDIR

cd $APPDIR
source $VENVDIR/bin/activate
python $APPDIR/wsapp.py > logs.txt 2>&1 & echo $! > app.pid

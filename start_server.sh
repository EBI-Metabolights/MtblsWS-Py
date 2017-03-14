#!/bin/bash

# check paths after installation and change accordingly
APPDIR=$HOME/metabolights/software/MtblsWS-Py
VENVDIR=$APPDIR/venv

PYTHONPATH=$APPDIR

cd $APPDIR
source $VENVDIR/bin/activate
python $APPDIR/app/wsapp.py > logs.txt 2>&1 & echo $! > app.pid

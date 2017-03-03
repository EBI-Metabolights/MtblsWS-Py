#!/bin/bash

# check paths after installation and change accordingly
APPDIR=$HOME/metabolights/software/MtblsWS-Py
VENVDIR=$APPDIR/venv

cd $APPDIR
source $VENVDIR/bin/activate
python $APPDIR/runWS.py > logs.txt 2>&1 & echo $! > app.pid
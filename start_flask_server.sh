#!/bin/bash

APPDIR=$PWD
VENVDIR=$APPDIR/venv351

PYTHONPATH=$APPDIR

cd $APPDIR

# activate RH collections for Python 3.5.1
source scl_source enable rh-python35

# activate Python virtual environment and launch WS
source $VENVDIR/bin/activate

# HACK, we need new HR7 servers
alias sqlite3='/net/isilonP/public/rw/homes/tc_cm01/metabolights/software/sqlite3/sqlite3'
cp /net/isilonP/public/rw/homes/tc_cm01/metabolights/software/ws-py/dev/MtblsWS-Py/triplelite.py /nfs/www-prod/web_hx2/cm/metabolights/software/ws-py/dev/MtblsWS-Py/venv351/lib64/python3.5/site-packages/owlready2/triplelite.py

python $APPDIR/wsapp.py > logs.txt 2>&1 & echo $! > app.pid

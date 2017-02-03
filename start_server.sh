#!/bin/bash

# check paths after installation and change accordingly
if [[ $1 == "dev" ]]
  then
    HOME=/home/tc_cm01
 else
    HOME=/net/isilonP/public/rw/homes/tc_cm01    
fi
APPDIR=$HOME/metabolights/software/MtblsWS-Py
VENVDIR=$APPDIR/venv

cd $APPDIR
source $VENVDIR/bin/activate
python $APPDIR/mtblsWS-Py.py

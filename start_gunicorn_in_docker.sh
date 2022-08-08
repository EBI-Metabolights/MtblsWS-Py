#!/bin/bash

HOST=$(hostname)
echo "Starting Green Unicorn server on host $HOST "

APPDIR=$PWD

mkdir -p $APPDIR/logs
PYTHONPATH="$APPDIR:$PYTHONPATH":
LOG=$APPDIR/logs/gunicorn_$HOST

#launch WS
gunicorn --workers 3 --threads 2 -b 0.0.0.0:5005 --worker-class gevent --pid ./app_$HOST.pid --preload wsapp:app

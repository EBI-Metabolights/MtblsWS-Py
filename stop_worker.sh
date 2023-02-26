#!/bin/bash

PROCESS_ID=$(ps -aux | grep 'python -m celery -A app.tasks.worker:celery worker --loglevel info -Q mtbls-tasks --autoscale' | awk '{ print $2 }' | tr '\n' ' ')
echo "PROCESS_ID: ${PROCESS_ID}"
kill -9 $PROCESS_ID
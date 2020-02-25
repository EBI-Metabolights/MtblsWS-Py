#!/usr/bin/env bash

HOST=$(hostname)
# read the process ID from the file
# and send a soft termination signal
kill -15 `cat app_${HOST}.pid`
mv app_${HOST}.pid app_${HOST}.pid.old

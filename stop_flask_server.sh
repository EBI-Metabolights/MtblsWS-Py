#!/usr/bin/env bash

# read the process ID from the file
# and send a soft termination signal
kill -15 `cat app.pid`
mv app.pid app-pid.old

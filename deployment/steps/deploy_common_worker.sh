#!/bin/bash

FILE_PATH=$(dirname "$0")
PARENT_PATH=$(dirname "$FILE_PATH")

echo "Parent path: $PARENT_PATH"
bash $PARENT_PATH/deploy.sh metablsws-py-common-worker "$APP_VERSION"
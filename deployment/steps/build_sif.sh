#!/bin/bash

FILE_PATH=$(dirname "$0")
echo "Current path: $PARENT_PATH"
PARENT_PATH=$(dirname "$FILE_PATH")
echo "Parent path: $PARENT_PATH"
source $PARENT_PATH/configuration.sh "$CI_COMMIT_REF_NAME" "$APP_VERSION"

singularity build $SIF_FILE_NAME docker-daemon://$IMAGE_NAME

if [ $? -eq 0 ]; then
    echo "Singularity build script is executed "
else
    echo "Singularity build script execution is failed"
    exit 1
fi

echo "$SIF_FILE_URL" 
curl --header "JOB-TOKEN: $CI_JOB_TOKEN" --upload-file $SIF_FILE_NAME "$SIF_FILE_URL"
echo "$SIF_LATEST_FILE_URL" 
curl --header "JOB-TOKEN: $CI_JOB_TOKEN" --upload-file $SIF_FILE_NAME "$SIF_LATEST_FILE_URL"

if [ $? -eq 0 ]; then
    echo " Singularity file upload completed "
else
    echo " Singularity file upload failed"
    exit 1
fi
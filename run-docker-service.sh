SERVICE_NAME=metablws-py
SERVICE_COUNT=$(sudo docker service ls | grep $SERVICE_NAME | wc -l)

if [ "$SERVICE_COUNT" -gt 0 ]
then
  echo "Current service will be restarted"
  sudo docker service rm $SERVICE_NAME
fi

sudo docker service create --name $SERVICE_NAME \
    --mount src=$(pwd)/,dst=/app-root,type=bind \
    --mount src=/home/ozgur/work/metabolights/dev/queue/,dst=/shared-folders/queue,type=bind \
    --mount src=/home/ozgur/work/metabolights/dev/studies/,dst=/shared-folders/studies,type=bind \
    --mount src=/home/ozgur/work/metabolights/dev/ftp/,dst=/shared-folders/ftp,type=bind \
    --mount src=/home/ozgur/work/metabolights/dev/chebi/,dst=/shared-folders/chebi,type=bind \
    --mount src=/home/ozgur/work/metabolights/dev/isatab/,dst=/shared-folders/isatab,type=bind  \
    --mount src=/home/ozgur/work/metabolights/dev/validation/,dst=/shared-folders/validation,type=bind \
    -e INSTANCE_DIR="/app-root/docker-instance" \
    --publish published=9999,target=5000 \
    --replicas 3 \
    metablws-py
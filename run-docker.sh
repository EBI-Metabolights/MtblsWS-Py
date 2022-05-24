sudo docker stop metablws-py
sudo docker rm metablws-py
sudo docker run --name metablws-py \
    --mount src=$(pwd)/,dst=/app-root,type=bind \
    --mount src=/home/ozgur/work/metabolights/dev/queue/,dst=/shared-folders/queue/,type=bind \
    --mount src=/home/ozgur/work/metabolights/dev/studies/,dst=/shared-folders/studies/,type=bind \
    --mount src=/home/ozgur/work/metabolights/dev/ftp/,dst=/shared-folders/ftp/,type=bind \
    --mount src=/home/ozgur/work/metabolights/dev/chebi/,dst=/shared-folders/chebi/,type=bind \
    --mount src=/home/ozgur/work/metabolights/dev/isatab/,dst=/shared-folders/isatab/,type=bind  \
    --mount src=/home/ozgur/work/metabolights/dev/validation/,dst=/shared-folders/validation/,type=bind \
    -e INSTANCE_DIR="$(pwd)/docker-instance" \
    -p 9999:5000 \
    -d \
    metablws-py
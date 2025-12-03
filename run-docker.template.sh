sudo docker stop metablsws-py
sudo docker rm metablsws-py

# sudo docker build -t dockerhub.ebi.ac.uk/mtbls/apps/metablsws-py:dev-latest .
# Replace define_actual_parent_folder with actual mounted folders
# and create docker instance folder with name $(pwd)/docker-instance or change script
sudo docker run --name metablsws-py \
    --mount src=define_actual_parent_folder/queue/,dst=/shared-folders/queue/,type=bind \
    --mount src=define_actual_parent_folder/studies/,dst=/shared-folders/studies/,type=bind \
    --mount src=define_actual_parent_folder/ftp/,dst=/shared-folders/ftp/,type=bind \
    --mount src=define_actual_parent_folder/chebi/,dst=/shared-folders/chebi/,type=bind \
    --mount src=define_actual_parent_folder/isatab/,dst=/shared-folders/isatab/,type=bind  \
    --mount src=define_actual_parent_folder/validation/,dst=/shared-folders/validation/,type=bind \
    --mount src=$(pwd)/docker-instance,dst=/app-root/instance,type=bind \
    -e INSTANCE_DIR="/app-root/instance" \
    -p 5000:5000 \
    -d \
    dockerhub.ebi.ac.uk/mtbls/apps/metablsws-py:dev-latest

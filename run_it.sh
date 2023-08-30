
SECRETS_DIR=/nfs/public/rw/metabolomics/development/ozgur/apps/environment/dev/metablsws-py/standalone/.secrets 
CONFIGS_DIR=/nfs/public/rw/metabolomics/development/ozgur/apps/environment/dev/metablsws-py/standalone/configs
PYTHONPATH=/nfs/public/rw/metabolomics/development/ozgur/MtblsWS-Py

eval "$(conda shell.bash hook)"
conda activate python38-MtblsWS

python3 scripts/create_test_data_files.py /nfs/public/services/metabolomics/prod/studies/stage/private /nfs/public/rw/metabolomics/dev/studies/maintenance_test/test-data-files

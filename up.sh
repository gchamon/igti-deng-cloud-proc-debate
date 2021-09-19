source '.env'

gcloud dataproc clusters create cluster-debate \
--enable-component-gateway \
--region us-east1 --zone us-east1-c \
--master-machine-type n1-standard-4 \
--master-boot-disk-size 500 \
--num-workers 2 \
--worker-machine-type n1-standard-4 \
--worker-boot-disk-size 500 \
--image-version 2.0-debian10 \
--optional-components JUPYTER \
--project igti-deng-cloud-proc

gsutil mb -p igti-deng-cloud-proc gs://$BUCKET_NAME

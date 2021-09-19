source '.env'

gcloud dataproc clusters delete cluster-debate \
--region us-east1 \
--project igti-deng-cloud-proc

gsutil rb -p igti-deng-cloud-proc gs://$BUCKET_NAME

source .env

files=($(ls *.tar.gz))
for file in "${files[@]}"; do
  tar -xzvf "$file"
done

gsutil cp *.csv gs://$BUCKET_NAME

rm *.csv

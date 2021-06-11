# BigQuery - TPC-DS

## How to use
```sh
export GOOGLE_APPLICATION_CREDENTIALS=GOOGLE_CREDENTIAL_FILE_PATH
go build -mod vendor
./bigquery \
  -path "TPC_DS_FILE_DIR" \
  -o "OUTPUT_DIR" \
  -p "BIGQUERY_PROJECT_ID" \
  -d "BIGQUERY_DATASET_ID"
```

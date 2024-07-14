#set -o xtrace

usage() { echo "Usage: $0 <output_bucket>" 1>&2; exit 1; }

output_bucket=$1

if [ -z "${output_bucket}" ]; then
    usage
fi

start_date=$2
end_date=$3
filter_date=false
if [ -n "${start_date}" ] && [ -n "${end_date}" ]; then
    filter_date=true
fi

# The tables below contain columns with type ARRAY<...>.
# BigQuery can't export it to CSV so we need to flatten it.
export_temp_dataset="export_temp_dataset"
export_temp_blocks_table="flattened_blocks"
export_temp_transactions_table="flattened_transactions"
export_temp_logs_table="flattened_logs"
export_temp_contracts_table="flattened_contracts"

export_temp_token_transfers_table="flattened_token_transfers"
export_temp_traces_table="flattened_traces"
export_temp_tokens_table="flattened_tokens"

bq rm -r -f ${export_temp_dataset}
bq mk ${export_temp_dataset}

flatten_table() {
    local sql_file=$1
    local temp_table_name=$2
    local timestamp_column=$3
    local sql=$(cat ./${sql_file} | awk -F '--' '{print $1}'| tr '\n' ' ')

    if [ "${filter_date}" = "true" ]; then
        sql="${sql} where date(${timestamp_column}) >= '${start_date}' and date(${timestamp_column}) <= '${end_date}'"
    fi

    echo "Executing query ${sql}"
    bq --location=US query --destination_table ${export_temp_dataset}.${temp_table_name} --use_legacy_sql=false "${sql}"
}

flatten_table "flatten_crypto_ethereum_blocks.sql" "${export_temp_blocks_table}" "timestamp"
flatten_table "flatten_crypto_ethereum_transactions.sql" "${export_temp_transactions_table}" "block_timestamp"
flatten_table "flatten_crypto_ethereum_logs.sql" "${export_temp_logs_table}" "block_timestamp"
flatten_table "flatten_crypto_ethereum_contracts.sql" "${export_temp_contracts_table}" "block_timestamp"

flatten_table "flatten_crypto_ethereum_token_transfers.sql" "${export_temp_token_transfers_table}" "block_timestamp"
flatten_table "flatten_crypto_ethereum_traces.sql" "${export_temp_traces_table}" "block_timestamp"
flatten_table "flatten_crypto_ethereum_tokens.sql" "${export_temp_tokens_table}" "block_timestamp"

declare -a tables=(
    "${export_temp_dataset}.${export_temp_blocks_table}"
    "${export_temp_dataset}.${export_temp_transactions_table}"
    "${export_temp_dataset}.${export_temp_token_transfers_table}"
    "${export_temp_dataset}.${export_temp_traces_table}"
    "${export_temp_dataset}.${export_temp_tokens_table}"
    "${export_temp_dataset}.${export_temp_logs_table}"
    "${export_temp_dataset}.${export_temp_contracts_table}"
)

for table in "${tables[@]}"
do
    echo "Exporting BigQuery table ${table}"
    if [ "${filter_date}" = "true" ]; then
        query="select * from \`${table//:/.}\`"
        timestamp_column="block_timestamp"
        if [ "${table}" = "${export_temp_dataset}.${export_temp_blocks_table}" ]; then
            timestamp_column="timestamp"
        fi
        query="${query} where date(${timestamp_column}) >= '${start_date}' and date(${timestamp_column}) <= '${end_date}'"
        filtered_table_name="${table//[.:-]/_}_filtered"
        echo "Executing query ${query}"
        bq --location=US query --destination_table "${export_temp_dataset}.${filtered_table_name}" --use_legacy_sql=false "${query}"

        output_folder=${filtered_table_name}
        bash bigquery_to_gcs.sh "${export_temp_dataset}.${filtered_table_name}" ${output_bucket} ${output_folder}
        #gsutil -m mv gs://${output_bucket}/${output_folder}/* gs://${output_bucket}/${table}/
        gsutil -m mv gs://${output_bucket}/${output_folder}/* gs://${output_bucket}/${start_date}-${end_date}/${table}/
    else
        output_folder=${table}
        bash bigquery_to_gcs.sh ${table} ${output_bucket} ${output_folder}
    fi
done

# Rename output folder for flattened tables
gsutil -m mv gs://${output_bucket}/${start_date}-${end_date}/${export_temp_dataset}.${export_temp_blocks_table}/* gs://${output_bucket}/${start_date}-${end_date}/bigquery-public-data:crypto_ethereum.blocks/
gsutil -m mv gs://${output_bucket}/${start_date}-${end_date}/${export_temp_dataset}.${export_temp_transactions_table}/* gs://${output_bucket}/${start_date}-${end_date}/bigquery-public-data:crypto_ethereum.transactions/
gsutil -m mv gs://${output_bucket}/${start_date}-${end_date}/${export_temp_dataset}.${export_temp_logs_table}/* gs://${output_bucket}/${start_date}-${end_date}/bigquery-public-data:crypto_ethereum.logs/
gsutil -m mv gs://${output_bucket}/${start_date}-${end_date}/${export_temp_dataset}.${export_temp_contracts_table}/* gs://${output_bucket}/${start_date}-${end_date}/bigquery-public-data:crypto_ethereum.contracts/

gsutil -m mv gs://${output_bucket}/${start_date}-${end_date}/${export_temp_dataset}.${export_temp_token_transfers_table}/* gs://${output_bucket}/${start_date}-${end_date}/bigquery-public-data:crypto_ethereum.token_transfers/
gsutil -m mv gs://${output_bucket}/${start_date}-${end_date}/${export_temp_dataset}.${export_temp_traces_table}/* gs://${output_bucket}/${start_date}-${end_date}/bigquery-public-data:crypto_ethereum.traces/
gsutil -m mv gs://${output_bucket}/${start_date}-${end_date}/${export_temp_dataset}.${export_temp_tokens_table}/* gs://${output_bucket}/${start_date}-${end_date}/bigquery-public-data:crypto_ethereum.tokens/

# Cleanup
bq rm -r -f ${export_temp_dataset}

db_name_with_path=$4
gsutil -m cp -r gs://${output_bucket}/${start_date}-${end_date} /home/ubuntu/ethereum_data

for dir in "/home/ubuntu/ethereum_data/${start_date}-${end_date}"/*
do
    table_name="${dir##*.}"
    echo "Processing directory $dir : $table_name"
    for file in "$dir"/*
    do
        filename=$(basename "$file")
        filename_without_ext="${filename%.*}"
        destination_file="$dir/$filename_without_ext.csv"
        gunzip -c "$file" > "${destination_file}"
        sudo sqlite3 ${db_name_with_path} <<EOF
.mode csv
.import "${destination_file}" $table_name
EOF
rm $file
: > $destination_file
    done
done
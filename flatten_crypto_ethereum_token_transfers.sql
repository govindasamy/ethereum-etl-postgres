select 
    token_address,
    from_address,
    to_address,
    value,
    transaction_hash,
    log_index,
    datetime(block_timestamp) AS block_timestamp,
    block_number,
    block_hash
from `bigquery-public-data.crypto_ethereum.token_transfers`
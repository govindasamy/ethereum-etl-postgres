select
    datetime(timestamp) AS timestamp,
    number,
    `hash`,
    parent_hash,
    nonce,
    sha3_uncles,
    logs_bloom,
    transactions_root,
    state_root,
    receipts_root,
    miner,
    difficulty,
    total_difficulty,
    size,
    extra_data,
    gas_limit,
    gas_used,
    transaction_count,
    base_fee_per_gas,
    withdrawals_root,
    blob_gas_used,
    excess_blob_gas
from `bigquery-public-data.crypto_ethereum.blocks`
select
    address,
    name,
    symbol,
    decimals,
    total_supply,
    block_number,
    block_hash,
    datetime(block_timestamp) AS block_timestamp
from `bigquery-public-data.crypto_ethereum.tokens`
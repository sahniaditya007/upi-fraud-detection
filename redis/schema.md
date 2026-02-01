## Payer velocity
payer:{upi_id}:tx_count:60s
payer:{upi_id}:amount_sum:60s

## Payer spread
payer:{upi_id}:unique_payees:600s (SET)

## Payee aggregation (mule detection)
payee:{upi_id}:unique_payers:600s (SET)

# USER BEHAVIOR
user:{user_id}:tx_count:60s        -> INTEGER (TTL 60s)
user:{user_id}:receivers:60s       -> SET (TTL 60s)
user:{user_id}:beneficiaries       -> SET (TTL 30 days)
user:{user_id}:last_tx_ts          -> TIMESTAMP (no TTL)

# RECEIVER BEHAVIOR
receiver:{receiver_id}:senders:24h -> SET (TTL 24h)
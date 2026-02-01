##Payer velocity
payer:{upi_id}:tx_count:60s
payer:{upi_id}:amount_sum:60s

##Payer spread
payer:{upi_id}:unique_payees:600s (SET)

##Payee aggregation (mule detection)
payee:{upi_id}:unique_payers:600s (SET)
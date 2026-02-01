from config.redis import get_redis_client

redis_client = get_redis_client()


def update_redis_state(event):
    payer = event["payer_upi_id"]
    payee = event["payee_upi_id"]
    amount = event["amount"]

    # -------- PAYER velocity --------
    tx_count_key = f"payer:{payer}:tx_count:60s"
    amount_sum_key = f"payer:{payer}:amount_sum:60s"

    redis_client.incr(tx_count_key)
    redis_client.incrby(amount_sum_key, amount)

    redis_client.expire(tx_count_key, 60)
    redis_client.expire(amount_sum_key, 60)

    # -------- PAYER spread --------
    payer_spread_key = f"payer:{payer}:unique_payees:600s"
    redis_client.sadd(payer_spread_key, payee)
    redis_client.expire(payer_spread_key, 600)

    # -------- PAYEE aggregation (mule signal) --------
    payee_agg_key = f"payee:{payee}:unique_payers:600s"
    redis_client.sadd(payee_agg_key, payer)
    redis_client.expire(payee_agg_key, 600)

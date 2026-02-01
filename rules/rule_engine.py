from config.redis import get_redis_client

redis_client = get_redis_client()


def get_int(key):
    val = redis_client.get(key)
    return int(val) if val else 0


def get_set_size(key):
    return redis_client.scard(key)


def evaluate_rules(event):

    payer = event["payer_upi_id"]
    payee = event["payee_upi_id"]
    amount = event["amount"]

    risk_score = 0
    triggered_rules = []

    # ---------- Rule 1: High velocity payer ----------
    tx_count_60s = get_int(f"payer:{payer}:tx_count:60s")
    if tx_count_60s >= 5:
        risk_score += 25
        triggered_rules.append("HIGH_TX_VELOCITY")

    # ---------- Rule 2: First-time high amount ----------
    if amount >= 10000 and tx_count_60s <= 1:
        risk_score += 30
        triggered_rules.append("FIRST_TIME_HIGH_AMOUNT")

    # ---------- Rule 3: New beneficiary spread ----------
    unique_payees_10m = get_set_size(f"payer:{payer}:unique_payees:600s")
    if unique_payees_10m >= 5:
        risk_score += 20
        triggered_rules.append("RAPID_NEW_BENEFICIARIES")

    # ---------- Rule 4: Mule account detection ----------
    unique_payers_10m = get_set_size(f"payee:{payee}:unique_payers:600s")
    if unique_payers_10m >= 10:
        risk_score += 35
        triggered_rules.append("POTENTIAL_MULE_ACCOUNT")

    return min(risk_score, 100), triggered_rules

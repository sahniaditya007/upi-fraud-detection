import numpy as np
from datetime import datetime


def extract_features(transaction: dict, redis_state: dict, rule_score: int):
    """
    Convert transaction + state into ML feature vector
    """

    event_time_ms = transaction.get("event_time")
    if event_time_ms:
        hour = datetime.fromtimestamp(event_time_ms / 1000).hour
    else:
        hour = datetime.utcnow().hour

    features = [
        transaction["amount"],
        redis_state.get("txn_count_5m", 0),
        redis_state.get("total_amount_5m", 0),
        redis_state.get("unique_payees_5m", 0),
        int(redis_state.get("is_new_beneficiary", False)),
        hour,
        rule_score,
    ]

    return np.array(features)

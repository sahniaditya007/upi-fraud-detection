import yaml
from datetime import datetime


RULES_PATH = "config/fraud_rules.yaml"


class RuleEngine:
    def __init__(self):
        with open(RULES_PATH, "r") as f:
            self.config = yaml.safe_load(f)

        self.rules = self.config["rules"]
        self.alert_threshold = self.config["risk"]["alert_threshold"]

    def evaluate(self, transaction: dict, redis_state: dict):
        """
        Evaluate transaction against all Tier-1 fraud rules.
        """

        risk_score = 0
        triggered_rules = []

        # -------------------------------
        # Rule 1: High velocity (burst)
        # -------------------------------
        velocity = self.rules["velocity"]
        txn_count = redis_state.get("txn_count_5m", 0)

        if txn_count > velocity["max_txns"]:
            risk_score += velocity["score"]
            triggered_rules.append("HIGH_VELOCITY")

        # -------------------------------
        # Rule 2: High total amount burst
        # -------------------------------
        high_amount = self.rules["high_amount"]
        total_amount = redis_state.get("total_amount_5m", 0)

        if total_amount >= high_amount["threshold"]:
            risk_score += high_amount["score"]
            triggered_rules.append("HIGH_AMOUNT_BURST")

        # -------------------------------
        # Rule 3: New beneficiary
        # -------------------------------
        if redis_state.get("is_new_beneficiary", False):
            rule = self.rules["new_beneficiary"]
            risk_score += rule["score"]
            triggered_rules.append("NEW_BENEFICIARY")

        # -------------------------------
        # Rule 4: Late-night transaction
        # -------------------------------
        night = self.rules["night_transaction"]

        event_time_ms = transaction.get("event_time")
        if event_time_ms:
            txn_time = datetime.fromtimestamp(event_time_ms / 1000)
        else:
            txn_time = datetime.utcnow()

        hour = txn_time.hour

        if night["start_hour"] <= hour <= night["end_hour"]:
            risk_score += night["score"]
            triggered_rules.append("NIGHT_TRANSACTION")

        # -------------------------------
        # Rule 5: Mule / fan-in detection
        # -------------------------------
        mule = self.rules["mule_pattern"]
        unique_senders = redis_state.get("unique_senders", 0)

        if unique_senders >= mule["unique_senders_threshold"]:
            risk_score += mule["score"]
            triggered_rules.append("MULE_ACCOUNT_PATTERN")

        return risk_score, triggered_rules

    def should_alert(self, risk_score: int) -> bool:
        return risk_score >= self.alert_threshold

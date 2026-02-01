import json
import csv
from collections import defaultdict

from rules.rule_engine import RuleEngine
from ml.features import extract_features


INPUT_FILE = "data/transactions.jsonl"
OUTPUT_FILE = "data/training.csv"


def generate():
    engine = RuleEngine()

    # -------------------------------------------------
    # LOCAL rolling state (offline approximation of Redis)
    # -------------------------------------------------
    txn_count_5m = defaultdict(int)
    total_amount_5m = defaultdict(float)
    unique_payees_5m = defaultdict(set)

    with open(INPUT_FILE, "r") as fin, open(OUTPUT_FILE, "w", newline="") as fout:
        writer = csv.writer(fout)

        # CSV header
        writer.writerow([
            "amount",
            "txn_count_5m",
            "total_amount_5m",
            "unique_payees_5m",
            "is_new_beneficiary",
            "hour",
            "rule_score",
            "label",
        ])

        for line in fin:
            txn = json.loads(line)

            payer = txn["payer_upi_id"]
            payee = txn["payee_upi_id"]

            # -------------------------------------------------
            # Update NORMAL rolling behavior
            # -------------------------------------------------
            txn_count_5m[payer] += 1
            total_amount_5m[payer] += txn["amount"]
            unique_payees_5m[payer].add(payee)

            # -------------------------------------------------
            # FRAUD SEEDING (BOOTSTRAP ONLY – OFFLINE ML)
            #  ~10% transactions become fraud-like
            # -------------------------------------------------
            is_seeded_fraud = hash(txn["event_id"]) % 10 == 0

            if is_seeded_fraud:
                # Force fraud-like behavior
                redis_state = {
                    "txn_count_5m": 15,
                    "total_amount_5m": txn["amount"] * 10,
                    "unique_payees_5m": 7,
                    "is_new_beneficiary": True,
                    "unique_senders": 5,
                }
            else:
                redis_state = {
                    "txn_count_5m": txn_count_5m[payer],
                    "total_amount_5m": total_amount_5m[payer],
                    "unique_payees_5m": len(unique_payees_5m[payer]),
                    "is_new_beneficiary": txn_count_5m[payer] == 1,
                    "unique_senders": 1,
                }

            # -------------------------------------------------
            # Rule-based scoring
            # -------------------------------------------------
            rule_score, _ = engine.evaluate(txn, redis_state)

            # -------------------------------------------------
            # Feature extraction
            # -------------------------------------------------
            features = extract_features(txn, redis_state, rule_score)

            # -------------------------------------------------
            # LABEL (explicit, honest bootstrapping)
            # -------------------------------------------------
            label = 1 if is_seeded_fraud else 0

            writer.writerow(list(features) + [label])


if __name__ == "__main__":
    generate()
    print("Training dataset generated.")

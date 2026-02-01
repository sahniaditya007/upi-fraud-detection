import time
import redis

# -----------------------------
# Redis configuration
# -----------------------------
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    decode_responses=True,
)

# Sliding window
WINDOW_SECONDS = 300


# -----------------------------
# Redis key helpers
# -----------------------------
def _txn_count_key(payer):
    return f"txn:count:{payer}"


def _txn_amount_key(payer):
    return f"txn:amount:{payer}"


def _unique_payee_key(payer):
    return f"txn:payees:{payer}"


def _last_txn_ts_key(payer):
    return f"txn:last_ts:{payer}"


# -----------------------------
# Update Redis state
# -----------------------------
def update_redis_state(event):
    payer = event["payer_upi_id"]
    payee = event["payee_upi_id"]
    amount = float(event["amount"])

    now = int(time.time())

    pipe = r.pipeline()

    # Increment txn count
    pipe.incr(_txn_count_key(payer))
    pipe.expire(_txn_count_key(payer), WINDOW_SECONDS)

    # Add amount
    pipe.incrbyfloat(_txn_amount_key(payer), amount)
    pipe.expire(_txn_amount_key(payer), WINDOW_SECONDS)

    # Track unique payees
    pipe.sadd(_unique_payee_key(payer), payee)
    pipe.expire(_unique_payee_key(payer), WINDOW_SECONDS)

    # Track last transaction time
    pipe.set(_last_txn_ts_key(payer), now)
    pipe.expire(_last_txn_ts_key(payer), WINDOW_SECONDS)

    pipe.execute()


# -----------------------------
# Fetch Redis-derived features
# -----------------------------
def get_redis_state(event):
    payer = event["payer_upi_id"]

    txn_count = r.get(_txn_count_key(payer))
    total_amount = r.get(_txn_amount_key(payer))
    unique_payees = r.scard(_unique_payee_key(payer))
    last_ts = r.get(_last_txn_ts_key(payer))
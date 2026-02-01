import pickle
import numpy as np

MODEL_PATH = "ml/model.pkl"
SCALER_PATH = "ml/scaler.pkl"

with open(MODEL_PATH, "rb") as f:
    model = pickle.load(f)

with open(SCALER_PATH, "rb") as f:
    scaler = pickle.load(f)


def predict_proba(features: np.ndarray) -> float:
    """
    Returns fraud probability in range [0, 1]
    """
    X = scaler.transform([features])
    return model.predict_proba(X)[0][1]

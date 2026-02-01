import pandas as pd
import pickle

from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler


DATA_FILE = "data/training.csv"


def train():
    df = pd.read_csv(DATA_FILE)

    X = df.drop(columns=["label"]).values
    y = df["label"].values

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = LogisticRegression(
        class_weight="balanced",
        max_iter=500
    )

    model.fit(X_scaled, y)

    with open("ml/model.pkl", "wb") as f:
        pickle.dump(model, f)

    with open("ml/scaler.pkl", "wb") as f:
        pickle.dump(scaler, f)

    print("Model trained and saved.")


if __name__ == "__main__":
    train()

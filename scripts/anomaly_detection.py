import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.metrics import precision_score, recall_score
from pymongo import MongoClient
import psycopg2
import warnings
warnings.filterwarnings("ignore")


# --------------------------------------------
# DB CONNECTIONS (EDIT IF NEEDED)
# --------------------------------------------
MONGO_URI = "mongodb://localhost:27017"
POSTGRES_CONFIG = {
    "dbname": "transportdb",
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": 5432
}


# --------------------------------------------
# LOAD SAMPLE DATA
# --------------------------------------------
def load_data():
    df = pd.DataFrame({
        "temperature": [20, 21, 22, 23, 80, 21, 22],   # 80 is anomalous spike
        "fare": [10, 11, 9, 10, 500, 8, 12],           # 500 is outlier fare
        "humidity": [40, 42, None, 41, 43, 300, 44]    # None + 300 are anomalies
    })
    return df


# --------------------------------------------
# DETECT ANOMALIES
# --------------------------------------------
def detect_anomalies(df):
    df_clean = df.fillna(-999)

    model = IsolationForest(contamination=0.15, random_state=42)
    df["is_anomaly"] = model.fit_predict(df_clean)

    df["is_anomaly"] = df["is_anomaly"].apply(lambda x: 1 if x == -1 else 0)
    return df


# --------------------------------------------
# STORE ANOMALIES IN MONGODB
# --------------------------------------------
def store_in_mongo(anomalies):
    client = MongoClient(MONGO_URI)
    db = client["transportdb"]
    anomalies_col = db["Anomalies"]
    anomalies_col.insert_many(anomalies.to_dict(orient="records"))
    print("✔ Anomalies stored in MongoDB collection: Anomalies")


# --------------------------------------------
# STORE ANOMALIES IN POSTGRESQL
# --------------------------------------------
def store_in_postgres(anomalies):
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS anomalies (
            id SERIAL PRIMARY KEY,
            temperature FLOAT,
            fare FLOAT,
            humidity FLOAT,
            is_anomaly INT
        );
    """)

    for _, row in anomalies.iterrows():
        cur.execute(
            "INSERT INTO anomalies (temperature, fare, humidity, is_anomaly) VALUES (%s, %s, %s, %s)",
            (
                float(row["temperature"]) if row["temperature"] is not None else None,
                float(row["fare"]) if row["fare"] is not None else None,
                float(row["humidity"]) if row["humidity"] is not None else None,
                int(row["is_anomaly"])
            )
        )


    conn.commit()
    cur.close()
    conn.close()
    print("✔ Anomalies stored in PostgreSQL table: anomalies")


# --------------------------------------------
# PERFORMANCE METRICS (Precision & Recall)
# --------------------------------------------
def calculate_metrics(df):
    df["true_label"] = df["temperature"].apply(lambda x: 1 if x > 70 else 0)
    precision = precision_score(df["true_label"], df["is_anomaly"])
    recall = recall_score(df["true_label"], df["is_anomaly"])
    return precision, recall


# --------------------------------------------
# MAIN PIPELINE
# --------------------------------------------
def run_anomaly_detection():
    print("▶ Loading data...")
    df = load_data()

    print("▶ Detecting anomalies...")
    df = detect_anomalies(df)

    anomalies = df[df["is_anomaly"] == 1]

    print("▶ Storing anomalies in MongoDB and PostgreSQL...")
    store_in_mongo(anomalies)
    store_in_postgres(anomalies)

    print("▶ Calculating detection metrics...")
    precision, recall = calculate_metrics(df)

    print("\n===== ANOMALY DETECTION REPORT =====")
    print(df)
    print(f"\nPrecision: {precision}")
    print(f"Recall: {recall}")
    print("====================================\n")


if __name__ == "__main__":
    run_anomaly_detection()

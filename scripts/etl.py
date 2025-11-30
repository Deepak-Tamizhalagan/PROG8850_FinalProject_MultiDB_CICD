import os
import requests
import pandas as pd
from pymongo import MongoClient


def fetch_climate_data():
    url = "https://api.worldbank.org/v2/country/CAN/indicator/EN.CLC.MDAT.ZS?format=json"
    print(f"Fetching data from API: {url}")
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"API fetch failed with status code {response.status_code}")

    data = response.json()

    try:
        records = data[1]  # API returns in format [metadata, data]
        df = pd.DataFrame(records)
        print("Sample Climate Data (First 5 rows):")
        print(df.head())
        return df
    except Exception as e:
        print("Error processing API response:", e)
        raise


def insert_into_mongodb(df):
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        raise ValueError("MONGO_URI environment variable not set.")

    print("Connecting to MongoDB...")
    client = MongoClient(mongo_uri)
    db = client["climate_db"]  # DB name
    collection = db["weather_data"]  # collection name

    print("Inserting data into MongoDB...")
    data_to_insert = df.to_dict("records")
    collection.insert_many(data_to_insert)
    print(f"Inserted {len(data_to_insert)} records into MongoDB successfully!")


def main():
    print(" Starting ETL - Phase 1: Fetch Climate Data")
    df = fetch_climate_data()

    print(" Inserting into MongoDB...")
    insert_into_mongodb(df)

    print(" ETL Phase 1 complete – Data fetched and stored in MongoDB.")


if __name__ == "__main__":
    main()

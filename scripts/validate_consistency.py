import os
import sys
import mysql.connector
from pymongo import MongoClient

# Get MongoDB URI from command-line argument
def get_mongo_uri():
    if len(sys.argv) > 1:
        return sys.argv[1]  # Example: python validate_consistency.py mongodb://localhost:27017/
    raise ValueError("MongoDB URI must be provided as a command-line argument.")

def validate_data_consistency():
    # MySQL connection
    mysql_password = os.getenv("MYSQL_ROOT_PASSWORD")
    if not mysql_password:
        raise ValueError("MYSQL_ROOT_PASSWORD environment variable not set.")

    conn = mysql.connector.connect(
        host="127.0.0.1",  # Local service name in GitHub runner
        user="root",
        password=mysql_password,
        database="climate_db"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM weather_data")
    mysql_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    # MongoDB connection using CLI argument
    mongo_uri = get_mongo_uri()
    client = MongoClient(mongo_uri)
    mongo_count = client["climate_db"]["weather_data"].count_documents({})

    print("\n Data Consistency Check")
    print(f" MySQL record count:   {mysql_count}")
    print(f" MongoDB record count: {mongo_count}")

    if mysql_count == mongo_count:
        print("✔ Data is consistent between MySQL and MongoDB!")
    else:
        raise Exception(" Data mismatch detected between MySQL and MongoDB")

def main():
    print("\n Starting Data Validation...")
    validate_data_consistency()

if __name__ == "__main__":
    main()

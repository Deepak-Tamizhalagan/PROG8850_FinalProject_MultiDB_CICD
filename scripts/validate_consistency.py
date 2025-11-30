import os
import mysql.connector
from pymongo import MongoClient

def validate_data_consistency():
    # MySQL connection
    mysql_password = os.getenv("MYSQL_ROOT_PASSWORD")
    conn = mysql.connector.connect(
        host="mysql",
        user="root",
        password=mysql_password,
        database="climate_db"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM weather_data")
    mysql_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    # MongoDB connection
    mongo_uri = os.getenv("MONGO_URI")
    client = MongoClient(mongo_uri)
    mongo_count = client["climate_db"]["weather_data"].count_documents({})

    print("\n Data Consistency Check")
    print(f"MySQL record count:   {mysql_count}")
    print(f"MongoDB record count: {mongo_count}")

    if mysql_count == mongo_count:
        print(" Data is consistent between MySQL and MongoDB!")
    else:
        raise Exception(" Data mismatch detected between MySQL and MongoDB")


def main():
    print(" Starting Data Validation...")
    validate_data_consistency()


if __name__ == "__main__":
    main()

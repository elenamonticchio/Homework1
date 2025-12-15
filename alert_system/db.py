import os
import mysql.connector

def get_connection():
    return mysql.connector.connect(
        host=os.getenv("DATA_DB_HOST", "data-db"),
        user=os.getenv("DATA_DB_USER"),
        password=os.getenv("DATA_DB_PASSWORD"),
        database=os.getenv("DATA_DB_NAME"),
        port=int(os.getenv("DATA_DB_PORT", "3306"))
    )

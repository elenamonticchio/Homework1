import os
import mysql.connector
from mysql.connector import Error
import time

DB_HOST = os.getenv("DB_HOST", "user-db")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "user_db")
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "user_app_pwd")


def get_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def init_db():
    conn = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                email VARCHAR(255) PRIMARY KEY,
                full_name VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS request_log (
                request_id VARCHAR(255) PRIMARY KEY,
                response_json TEXT
                )
            """
        )

        conn.commit()
        print("Tabelle 'users' e 'request_log' pronte.")

    except Error as e:
        print(f"Errore durante init_db: {e}")

    finally:
        if conn:
            conn.close()

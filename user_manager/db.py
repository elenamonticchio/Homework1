import os
import mysql.connector
from mysql.connector import Error
import time

# Parametri (default o docker-compose)
DB_HOST = os.getenv("DB_HOST", "user-db")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "user_db")
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "user_app_pwd")


def get_connection():
    """
    Crea una connessione a MySQL.
    """
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def init_db():
    """
    Crea le tabelle necessarie:
    - users
    - request_log
    """
    conn = None

    # Pausa per permettere a MySQL di avviarsi
    time.sleep(15)

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Tabella utenti
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                                                 email VARCHAR(255) PRIMARY KEY,
                full_name VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        )

        # Tabella per idempotenza richieste
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS request_log (
                request_id VARCHAR(255) PRIMARY KEY,
                email VARCHAR(255),
                response_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

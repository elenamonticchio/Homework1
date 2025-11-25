import os
import mysql.connector
from mysql.connector import Error
import time

# Leggo i parametri dal docker-compose, ma metto anche dei DEFAULT
DB_HOST = os.getenv("DB_HOST", "user-db")          # nome del container MySQL
DB_PORT = int(os.getenv("DB_PORT", "3306"))        # porta interna del container
DB_NAME = os.getenv("DB_NAME", "user_db")          # nome del database
DB_USER = os.getenv("DB_USER", "user")             # utente MySQL
DB_PASSWORD = os.getenv("DB_PASSWORD", "user_app_pwd")  # password dell'utente


def get_connection():
    """
    Ritorna una nuova connessione a MySQL usando le variabili d'ambiente.
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
    Crea la tabella 'users' se non esiste,
    e stampa la lista delle tabelle per verifica.
    """
    conn = None

    # Attendo un attimo per sicurezza (MySQL potrebbe metterci un secondo ad avviarsi).
    time.sleep(15)

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Creazione tabella utenti
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                                                 email VARCHAR(255) PRIMARY KEY,
                full_name VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        )

        conn.commit()
        print("Tabella 'users' pronta.")

    except Error as e:
        print(f"❌ Errore durante init_db: {e}")

    finally:
        if conn:
            conn.close()

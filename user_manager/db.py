import os
import mysql.connector
from mysql.connector import Error

# Leggo i parametri dal docker-compose, ma metto anche un DEFAULT
# così il codice funziona anche se lanciato fuori da Docker.

DB_HOST = os.getenv("DB_HOST", "user-db")        # default = nome del container MySQL
DB_PORT = int(os.getenv("DB_PORT", "3306"))      # default = porta interna del container
DB_NAME = os.getenv("DB_NAME", "user_db")        # default = nome del database
DB_USER = os.getenv("DB_USER", "user")           # default = utente MySQL
DB_PASSWORD = os.getenv("DB_PASSWORD", "user_app_pwd")  # default = password dell'utente


def get_connection():
    """
    Ritorna una nuova connessione a MySQL usando le variabili d'ambiente.
    Se le env non esistono (es. in locale), usa i default sopra.
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
    Crea la tabella 'users' se non esiste.
    La chiameremo all'avvio dell'app Flask.
    """
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
        conn.commit()
        print("Tabella 'users' pronta.")
    except Error as e:
        print(f" Errore durante init_db: {e}")
    finally:
        if conn:
            conn.close()

import os
import mysql.connector
from mysql.connector import Error
import time

# Leggo i parametri dal docker-compose, ma metto anche un DEFAULT
# così il codice funziona anche se lanciato fuori da Docker.

DB_HOST = os.getenv("DB_HOST", "data-db")        # default = nome del container MySQL
DB_PORT = int(os.getenv("DB_PORT", "3306"))      # default = porta interna del container
DB_NAME = os.getenv("DB_NAME", "data_db")        # default = nome del database
DB_USER = os.getenv("DB_USER", "user")           # default = utente MySQL
DB_PASSWORD = os.getenv("DB_PASSWORD", "data_app_pwd")  # default = password dell'utente


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
    time.sleep(15)
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
        """ CREATE TABLE IF NOT EXISTS interests (
                email VARCHAR(255),
                airport VARCHAR(255),
                PRIMARY KEY (email, airport)
            ) """
        )

        cursor.execute(
        """ CREATE TABLE IF NOT EXISTS flights (
                flight_id INT AUTO_INCREMENT PRIMARY KEY,
                departure_airport VARCHAR(255),
                arrival_airport VARCHAR(255),
                date_time_arrival DATETIME,
                date_time_departure DATETIME
            ) """
        )


        conn.commit()
        time.sleep(3)
        print("Tabella 'interests' pronta.\n")
        time.sleep(3)
        print("Tabella 'flights' pronta.\n")

    except Error as e:
        print(f" Errore durante init_db: {e}")
    finally:
        if conn:
            conn.close()
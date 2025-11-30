import os
import mysql.connector
from mysql.connector import Error
import time

# Leggo i parametri dal docker-compose, ma metto anche un DEFAULT
# così il codice funziona anche se lanciato fuori da Docker.

DB_HOST = os.getenv("DB_HOST", "data-db")        # default = nome del container MySQL
DB_PORT = int(os.getenv("DB_PORT", "3306"))      # default = porta interna del container
DB_NAME = os.getenv("DB_NAME", "data_db")        # default = nome del database
DB_USER = os.getenv("DB_USER", "data_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "data_app_pwd")

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
        """ CREATE TABLE IF NOT EXISTS interests (
                email VARCHAR(255),
                airport VARCHAR(255),
                PRIMARY KEY (email, airport)
            ) """
        )

        cursor.execute(
        """ CREATE TABLE IF NOT EXISTS flights (
             flight_id VARCHAR(255) NOT NULL,
             departure_airport VARCHAR(255),
             arrival_airport VARCHAR(255),
             date_time_arrival DATETIME,
             date_time_departure DATETIME NOT NULL,
             PRIMARY KEY (flight_id, date_time_departure)
            );"""
        )

        conn.commit()
        print("Tabelle 'interests' e 'flights' pronte.\n")

    except Error as e:
        print(f" Errore durante init_db: {e}")
    finally:
        if conn:
            conn.close()
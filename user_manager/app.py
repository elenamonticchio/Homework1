import os
from flask import Flask, request, jsonify
from mysql.connector import Error

from db import get_connection, init_db

app = Flask(__name__)


@app.initialization
def setup():
    """
    Eseguito alla prima richiesta:
    crea la tabella 'users' se non esiste.
    """
    init_db()

# SERVE A VERIFICARE CHE IL SERVER FUNZIONI ( DA LEVARE O NO )
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


# ---------- AGGIUNTA UTENTE ----------
@app.route("/users", methods=["POST"])
def add_user():
    """
    Aggiunta di un Utente.
    Body JSON:
    {
      "email": "utente@example.com",
      "full_name": "Nome Cognome"
    }
    """
    data = request.get_json(silent=True) or {}
    email = data.get("email")
    full_name = data.get("full_name")

    # controllo campi obbligatori
    if not email or not full_name:
        return jsonify({"error": "email e full_name sono obbligatori"}), 400

    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Controllo se esiste già
        cursor.execute("SELECT email FROM users WHERE email = %s", (email,))
        existing = cursor.fetchone()
        if existing:
            return jsonify({"error": "Utente già registrato"}), 400

        # Inserimento nuovo utente
        cursor.execute(
            "INSERT INTO users (email, full_name) VALUES (%s, %s)",
            (email, full_name),
        )
        conn.commit()

        return jsonify({"message": "Utente inserito correttamente", "email": email}), 201

    except Error as e:
        print(f"Errore in add_user: {e}")
        return jsonify({"error": "Errore database"}), 500

    finally:
        if conn:
            conn.close()


# ---------- CANCELLAZIONE UTENTE ----------
@app.route("/users/<email>", methods=["DELETE"])
def delete_user(email):
    """
    Cancellazione di un Utente.
    URL: /users/<email>
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Controllo se l'utente esiste
        cursor.execute("SELECT email FROM users WHERE email = %s", (email,))
        existing = cursor.fetchone()
        if not existing:
            return jsonify({"error": "Utente non trovato"}), 404

        # Cancellazione
        cursor.execute("DELETE FROM users WHERE email = %s", (email,))
        conn.commit()

        return jsonify({"message": "Utente cancellato", "email": email}), 200

    except Error as e:
        print(f"Errore in delete_user: {e}")
        return jsonify({"error": "Errore database"}), 500

    finally:
        if conn:
            conn.close()





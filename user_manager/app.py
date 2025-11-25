import os
from flask import Flask, request, jsonify
from mysql.connector import Error

from db import get_connection, init_db

app = Flask(__name__)


# SERVE A VERIFICARE CHE IL SERVER FUNZIONI ( DA LEVARE O NO? )
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


# ---------- AGGIUNTA UTENTE ----------
@app.route("/users/add", methods=["POST"])
def add_user():
    data = request.get_json(silent=True) or {}
    email = data.get("email")
    full_name = data.get("full_name")

    if not email or not full_name:
        return jsonify({"error": "email e full_name sono obbligatori"}), 400

    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT email FROM users WHERE email = %s", (email,))
        existing = cursor.fetchone()
        if existing:
            return jsonify({"error": "Utente già registrato"}), 400

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


# ---------- LISTA UTENTI ----------
@app.route("/users", methods=["GET"])
def list_users():
    """
    Restituisce tutti gli utenti registrati nella tabella.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT email, full_name, created_at FROM users")
        users = cursor.fetchall()

        return jsonify(users), 200

    except Error as e:
        print(f"Errore in list_users: {e}")
        return jsonify({"error": "Errore database"}), 500

    finally:
        if conn:
            conn.close()


# ---------- CANCELLAZIONE UTENTE ----------
@app.route("/users/delete/<email>", methods=["DELETE"])
def delete_user(email):
    """
    Cancellazione di un Utente.
    URL: /users/<email>
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT email FROM users WHERE email = %s", (email,))
        existing = cursor.fetchone()
        if not existing:
            return jsonify({"error": "Utente non trovato"}), 404

        cursor.execute("DELETE FROM users WHERE email = %s", (email,))
        conn.commit()

        return jsonify({"message": "Utente cancellato", "email": email}), 200

    except Error as e:
        print(f"Errore in delete_user: {e}")
        return jsonify({"error": "Errore database"}), 500

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    init_db()
    listen_port = int(os.getenv("LISTEN_PORT", "5003"))
    app.run(host="0.0.0.0", port=listen_port) # 0.0.0.0 -> accetta richieste da qualsiasi indirizzo IP, port= listenport è la porta da cui il server ascolta le richieste, è la porta del container, non quella dell'host

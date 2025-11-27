import os
from flask import Flask, request, jsonify
from mysql.connector import Error
import json

from db import get_connection, init_db

app = Flask(__name__)

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


# ---------- REGISTRAZIONE UTENTE CON REQUEST-ID ----------
@app.route("/users/add", methods=["POST"])
def add_user():
    conn = None

    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        body = request.get_json(silent=True) or {}
        req_id = body.get("request_id")

        cursor.execute("SELECT 1 FROM request_log WHERE request_id = %s", (req_id,))
        if cursor.fetchone():
            resp = {
                "message": "Request ID esistente",
                "request_id": req_id
            }
            return jsonify(resp), 200

        email = body.get("email")
        full_name = body.get("full_name")

        if not email or not full_name:
            resp = {
                "error": "email e full_name sono obbligatori",
                "request_id": req_id
            }
            cursor.execute(
                "INSERT INTO request_log (request_id, response_json) VALUES (%s, %s)",
                (req_id, json.dumps(resp)),
            )
            conn.commit()
            return jsonify(resp), 400

        cursor.execute("SELECT email FROM users WHERE email = %s", (email,))
        if cursor.fetchone():
            resp = {
                "message": "Utente già inserito",
                "email": email,
                "request_id": req_id
            }

            cursor.execute(
                "INSERT INTO request_log (request_id, response_json) VALUES (%s, %s)",
                (req_id, json.dumps(resp)),
            )
            conn.commit()

            return jsonify(resp), 200

        cursor.execute(
            "INSERT INTO users (email, full_name) VALUES (%s, %s)",
            (email, full_name),
        )
        conn.commit()

        resp = {
            "message": "Utente inserito correttamente",
            "email": email,
            "request_id": req_id
        }

        cursor.execute(
            "INSERT INTO request_log (request_id, response_json) VALUES (%s, %s)",
            (req_id, json.dumps(resp)),
        )
        conn.commit()

        return jsonify(resp), 201

    except Error as e:
        print(f"Errore in add_user: {e}")
        return jsonify({"error": "Errore database"}), 500

    finally:
        if conn:
            conn.close()


# ---------- LISTA UTENTI ----------
@app.route("/users", methods=["GET"])
def list_users():
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

    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT email FROM users WHERE email = %s", (email,))
        if not cursor.fetchone():
            return jsonify({"error": "Utente non trovato"}), 404

        cursor.execute("DELETE FROM users WHERE email = %s", (email,))
        conn.commit()

        return jsonify({
            "message": "Utente cancellato",
            "email": email
        }), 200

    except Error as e:
        print(f"Errore in delete_user: {e}")
        return jsonify({"error": "Errore database"}), 500

    finally:
        if conn:
            conn.close()


# ---------- RUN SERVER ----------
if __name__ == "__main__":
    init_db()
    listen_port = int(os.getenv("LISTEN_PORT", "5003"))
    app.run(host="0.0.0.0", port=listen_port)


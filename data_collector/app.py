import os
from flask import Flask, request, jsonify
from mysql.connector import Error

from db import get_connection, init_db

app = Flask(__name__)

@app.route("/users/add-interest", methods=["POST"])
def add_interest():
    data = request.get_json(silent=True) or {}
    email = data.get("email")
    airport = data.get("airport")

    if not email or not airport:
        return jsonify({"error": "email e airport sono obbligatori"}), 400

    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT airport FROM interests WHERE email = %s AND airport = %s", (email, airport))
        existing = cursor.fetchone()
        if not existing:
            cursor.execute(
                "INSERT INTO interests (email, airport) VALUES (%s, %s)",
                (email, airport),
            )
        conn.commit()

        return jsonify({"message": "Interesse inserito correttamente", "airport": airport}), 201

    except Error as e:
        print(f"Errore in add_interest: {e}")
        return jsonify({"error": "Errore database"}), 500

    finally:
        if conn:
            conn.close()
@app.route("/interests", methods=["GET"])
def list_interests():
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT email, airport FROM interests")
        interests = cursor.fetchall()

        return jsonify(interests), 200

    except Error as e:
        print(f"Errore in list_interests: {e}")
        return jsonify({"error": "Errore database"}), 500

    finally:
        if conn:
            conn.close()



if __name__ == "__main__":
    init_db()
    listen_port = int(os.getenv("LISTEN_PORT", "5002"))
    app.run(host="0.0.0.0", port=listen_port)
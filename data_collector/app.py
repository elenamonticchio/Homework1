import os
from flask import Flask, request, jsonify
import requests
from mysql.connector import Error
from db import get_connection, init_db
from open_sky_token import get_token
from datetime import datetime, date, timedelta, time
from apscheduler.schedulers.background import BackgroundScheduler
app = Flask(__name__)

API_ROOT_URL = "https://opensky-network.org/api"

def save_flights_to_db(flights_data, requested_airport, flight_type):
    if not flights_data:
        return 0

    conn = None
    saved_count = 0

    column_mapping = {
        'flight_id': 'callsign',
        'departure_airport': 'estDepartureAirport',
        'arrival_airport': 'estArrivalAirport',
        'date_time_arrival': 'lastSeen',
        'date_time_departure': 'firstSeen'
    }

    try:
        conn = get_connection()
        cursor = conn.cursor()

        for flight in flights_data:
            flight_id = (flight.get(column_mapping['flight_id']) or '').strip()
            dep_airport = flight.get(column_mapping['departure_airport'])
            arr_airport = flight.get(column_mapping['arrival_airport'])
            arr_ts = flight.get(column_mapping['date_time_arrival'])
            dep_ts = flight.get(column_mapping['date_time_departure'])

            if (
                    not flight_id or
                    not dep_airport or
                    not arr_airport or
                    not isinstance(arr_ts, int) or
                    not isinstance(dep_ts, int)
            ):
                continue

            date_time_arrival = datetime.fromtimestamp(arr_ts)
            date_time_departure = datetime.fromtimestamp(dep_ts)

            query = """
                    INSERT IGNORE INTO flights (
                    flight_id, departure_airport, arrival_airport, date_time_arrival, date_time_departure
                ) VALUES (%s, %s, %s, %s, %s) 
                    """
            values = (
                flight_id,
                dep_airport,
                arr_airport,
                date_time_arrival,
                date_time_departure
            )

            cursor.execute(query, values)
            saved_count += cursor.rowcount

        conn.commit()
        return saved_count

    except Error as e:
        print(f"Errore database durante il salvataggio dei voli: {e}")
        return 0
    finally:
        if conn:
            conn.close()


def get_flights(airport_icao, access_token, flight_type):
    """
    flight_type: 'arrival' oppure 'departure'
    """
    yesterday = date.today() - timedelta(days=1)
    begin_time = int(datetime.combine(yesterday, time.min).timestamp())
    end_time = int(datetime.combine(yesterday, time.max).timestamp())

    params = {
        "airport": airport_icao,
        "begin": begin_time,
        "end": end_time
    }

    url = f"{API_ROOT_URL}/flights/{flight_type}"
    headers = {"Authorization": f"Bearer {access_token}"}

    try:
        response = requests.get(url, params=params, headers=headers, timeout=15)
        response.raise_for_status()

        if response.status_code == 204 or response.json() == []:
            return []

        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"Errore nella richiesta {flight_type} per {airport_icao}: {e}")
        return []


def get_interests():
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT DISTINCT airport FROM interests")

        unique_airports = [row[0] for row in cursor.fetchall()]
        return unique_airports
    except Error as e:
        print(f"Errore database nel recuperare interessi unici: {e}")
        return []
    finally:
        if conn:
            conn.close()


def get_open_sky_data():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    access_token = get_token()
    if not access_token:
        print("Impossibile ottenere il token.")
        return

    airports_to_monitor = get_interests()

    if not airports_to_monitor:
        print("Nessun aeroporto di interesse.")
        return

    total_saved_flights = 0

    print(f"  > Richiesta dati per aeroporti d'interesse...")

    for airport_icao in airports_to_monitor:
        flights_arr = get_flights(airport_icao, access_token, "arrival")
        flights_dep = get_flights(airport_icao, access_token, "departure")

        has_flights = False

        if flights_arr:
            total_saved_flights += save_flights_to_db(flights_arr, airport_icao, "arrival")
            has_flights = True

        if flights_dep:
            total_saved_flights += save_flights_to_db(flights_dep, airport_icao, "departure")
            has_flights = True

        if not has_flights:
            print(f"    > Nessun volo trovato per {airport_icao}.")

    print(f"[{timestamp}] Elaborazione completata. Totale nuovi voli: {total_saved_flights}.")

@app.route("/users/add-interests", methods=["POST"])
def add_interests():
    data = request.get_json(silent=True) or {}
    email = data.get("email")
    airports = data.get("airports")

    if not email:
        return jsonify({"error": "L'email è obbligatoria"}), 400

    if not isinstance(airports, list) or not airports:
        return jsonify({"error": "Il campo 'airports' deve essere una lista non vuota"}), 400

    conn = None
    inserted_airports = []

    try:
        conn = get_connection()
        cursor = conn.cursor()

        for airport_code in airports:
            cursor.execute(
                "SELECT airport FROM interests WHERE email = %s AND airport = %s",
                (email, airport_code)
            )

            existing = cursor.fetchone()

            if not existing:
                cursor.execute(
                    "INSERT INTO interests (email, airport) VALUES (%s, %s)",
                    (email, airport_code),
                )
                inserted_airports.append(airport_code)

        conn.commit()

        return jsonify({
            "message": "Interessi aggiornati",
            "inserted_airports": inserted_airports,
            "total_processed": len(airports)
        }), 201

    except Error as e:
        print(f"Errore in add_interests: {e}")
        return jsonify({"error": "Errore database"}), 500

    finally:
        if conn:
            conn.close()

@app.route("/users/remove-interests", methods=["DELETE"])
def remove_interests():
    data = request.get_json(silent=True) or {}
    email = data.get("email")
    airports = data.get("airports")

    if not email:
        return jsonify({"error": "L'email è obbligatoria"}), 400

    if not isinstance(airports, list) or not airports:
        return jsonify({"error": "Il campo 'airports' deve essere una lista non vuota"}), 400

    conn = None

    removed_airports = []

    try:
        conn = get_connection()
        cursor = conn.cursor()

        for airport_code in airports:
            cursor.execute(
                "DELETE FROM interests WHERE email = %s AND airport = %s",
                (email, airport_code)
            )

            if cursor.rowcount > 0:
                removed_airports.append(airport_code)

        conn.commit()

        return jsonify({
            "message": "Interessi rimossi",
            "removed_airports": removed_airports,
            "total_processed": len(airports)
        }), 200

    except Error as e:
        print(f"Errore in remove_interests: {e}")
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

@app.route("/flights", methods=["GET"])
def list_flights():
    conn = None

    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM flights")
        interests = cursor.fetchall()

        return jsonify(interests), 200

    except Error as e:
        print(f"Errore in list_flights: {e}")
        return jsonify({"error": "Errore database"}), 500

    finally:
        if conn:
            conn.close()

@app.route("/flights/latest", methods=["GET"])
def latest_flights():
    airport = request.args.get("airport")

    if not airport:
        return jsonify({"error": "Parametro 'airport' mancante"}), 400

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
                   SELECT *
                   FROM flights
                   WHERE arrival_airport = %s
                   ORDER BY date_time_arrival DESC
                       LIMIT 1;
                   """, (airport,))
    latest_arrival = cursor.fetchone()

    cursor.execute("""
                   SELECT *
                   FROM flights
                   WHERE departure_airport = %s
                   ORDER BY date_time_departure DESC
                       LIMIT 1;
                   """, (airport,))
    latest_departure = cursor.fetchone()

    conn.close()

    return jsonify({
        "arrival": latest_arrival,
        "departure": latest_departure
    }), 200

@app.route("/flights/avg", methods=["GET"])
def flights_average():
    airport = request.args.get("airport")
    days = request.args.get("days")

    if not airport or not days:
        return jsonify({"error": "Parametri 'airport' e 'days' obbligatori"}), 400

    try:
        days = int(days)
    except ValueError:
        return jsonify({"error": "'days' deve essere un numero intero"}), 400

    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
                       SELECT COUNT(*)
                       FROM flights
                       WHERE arrival_airport = %s
                         AND date_time_arrival >= NOW() - INTERVAL %s DAY
                       """, (airport, days))
        total_arrivals = cursor.fetchone()[0]
        avg_arrivals = total_arrivals / days if days > 0 else 0

        cursor.execute("""
                       SELECT COUNT(*)
                       FROM flights
                       WHERE departure_airport = %s
                         AND date_time_departure >= NOW() - INTERVAL %s DAY
                       """, (airport, days))
        total_departures = cursor.fetchone()[0]
        avg_departures = total_departures / days if days > 0 else 0

        conn.close()

        return jsonify({
            "airport": airport,
            "days": days,
            "arrivals": {
                "total": total_arrivals,
                "average_per_day": avg_arrivals
            },
            "departures": {
                "total": total_departures,
                "average_per_day": avg_departures
            }
        }), 200

    except Error as e:
        print("Errore flights-average:", e)
        return jsonify({"error": "Errore database"}), 500

@app.route("/flights/stats", methods=["GET"])
def flight_stats():
    airport = request.args.get("airport")
    days = request.args.get("days", "7")

    if not airport:
        return jsonify({"error": "Parametro 'airport' obbligatorio"}), 400

    try:
        days = int(days)
    except ValueError:
        return jsonify({"error": "'days' deve essere un numero intero"}), 400

    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
                       SELECT COUNT(*) AS total_arrivals
                       FROM flights
                       WHERE arrival_airport = %s
                         AND date_time_arrival >= NOW() - INTERVAL %s DAY
                       """, (airport, days))
        total_arrivals = cursor.fetchone()["total_arrivals"]

        cursor.execute("""
                       SELECT COUNT(*) AS total_departures
                       FROM flights
                       WHERE departure_airport = %s
                         AND date_time_departure >= NOW() - INTERVAL %s DAY
                       """, (airport, days))
        total_departures = cursor.fetchone()["total_departures"]

        cursor.execute("""
                       SELECT DATE(date_time_arrival) AS day,
                           COUNT(*) AS num_flights
                       FROM flights
                       WHERE arrival_airport = %s
                         AND date_time_arrival >= NOW() - INTERVAL %s DAY
                       GROUP BY day

                       UNION ALL

                       SELECT DATE(date_time_departure) AS day,
                           COUNT(*) AS num_flights
                       FROM flights
                       WHERE departure_airport = %s
                         AND date_time_departure >= NOW() - INTERVAL %s DAY
                       GROUP BY day
                       """, (airport, days, airport, days))

        day_counts = cursor.fetchall()

        busiest_day = None
        if day_counts:
            day_map = {}
            for row in day_counts:
                day = str(row["day"])
                day_map[day] = day_map.get(day, 0) + row["num_flights"]

            busiest_day = max(day_map.items(), key=lambda x: x[1])

        cursor.execute("""
                       SELECT HOUR(date_time_arrival) AS hour, COUNT(*) AS flights
                       FROM flights
                       WHERE arrival_airport = %s
                         AND date_time_arrival >= NOW() - INTERVAL %s DAY
                       GROUP BY hour

                       UNION ALL

                       SELECT HOUR(date_time_departure) AS hour, COUNT(*) AS flights
                       FROM flights
                       WHERE departure_airport = %s
                         AND date_time_departure >= NOW() - INTERVAL %s DAY
                       GROUP BY hour
                       """, (airport, days, airport, days))

        hour_counts = cursor.fetchall()

        busiest_hour = None
        if hour_counts:
            hour_map = {}
            for row in hour_counts:
                hour = int(row["hour"])
                hour_map[hour] = hour_map.get(hour, 0) + row["flights"]

            busiest_hour = max(hour_map.items(), key=lambda x: x[1])

        conn.close()

        return jsonify({
            "airport": airport,
            "days": days,
            "totals": {
                "arrivals": total_arrivals,
                "departures": total_departures
            },
            "busiest_day": {
                "day": busiest_day[0] if busiest_day else None,
                "flights": busiest_day[1] if busiest_day else None
            },
            "busiest_hour": {
                "hour": busiest_hour[0] if busiest_hour else None,
                "flights": busiest_hour[1] if busiest_hour else None
            }
        }), 200

    except Error as e:
        print("[DB ERROR flight_stats]", e)
        return jsonify({"error": "Errore database"}), 500
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    init_db()

    scheduler = BackgroundScheduler()

    scheduler.add_job(
        get_open_sky_data,
        'interval',
        hours=12,
        id='opensky_data_processing',
        replace_existing=True
    )
    scheduler.start()
    print("Scheduler APS avviato. Job di aggiornamento OpenSky pianificato ogni 12 ore.")

    get_token()
    get_open_sky_data()

    listen_port = int(os.getenv("LISTEN_PORT", "5002"))
    app.run(host="0.0.0.0", port=listen_port)
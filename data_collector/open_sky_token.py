import os
import time
import requests

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
CLIENT_ID = os.getenv("OPEN_SKY_CLIENT_ID")
CLIENT_SECRET = os.getenv("OPEN_SKY_CLIENT_SECRET")
CACHED_TOKEN = None
TOKEN_EXPIRATION_TIME = 0

def is_token_expired():
    return CACHED_TOKEN is None or time.time() >= (TOKEN_EXPIRATION_TIME - 60)

def get_opensky_token():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise ValueError("CLIENT_ID o CLIENT_SECRET non impostati")

    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }

    try:
        response = requests.post(
            TOKEN_URL,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10
        )
        response.raise_for_status()

        return response.json()

    except requests.RequestException as e:
        print(f"Errore nella richiesta del token: {e}")
        return None

def get_token():
    global CACHED_TOKEN, TOKEN_EXPIRATION_TIME

    if is_token_expired():
        token_data = get_opensky_token()
        if token_data and 'access_token' in token_data:
            CACHED_TOKEN = token_data['access_token']
            TOKEN_EXPIRATION_TIME = time.time() + token_data.get('expires_in', 1800)
            print("Token aggiornato!")

    print(f"Token attuale: {CACHED_TOKEN}")
    return CACHED_TOKEN
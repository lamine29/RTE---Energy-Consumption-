import os
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_PARAMS = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

CLIENT_ID = os.getenv("RTE_API_CLIENT_ID")
CLIENT_SECRET = os.getenv("RTE_API_CLIENT_SECRET")

def get_access_token():
    url = "https://digital.iservices.rte-france.com/token/oauth/"
    data = {"grant_type": "client_credentials"}
    response = requests.post(url, data=data, auth=(CLIENT_ID, CLIENT_SECRET))
    return response.json()["access_token"]

def fetch_data(token):
    url = "https://digital.iservices.rte-france.com/open_api/eco2mix-national-tr"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    return response.json()

def save_to_db(data):
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    for row in data["eco2mix"]:
        cur.execute(
            """
            INSERT INTO energy_mix (date, nuclear, wind, solar, hydro)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING;
            """,
            (
                row["date_heure"],
                row["nucleaire"],
                row["eolien"],
                row["solaire"],
                row["hydraulique"],
            )
        )

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    token = get_access_token()
    data = fetch_data(token)
    save_to_db(data)

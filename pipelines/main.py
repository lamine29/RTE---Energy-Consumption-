import os
import requests
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

DB_PARAMS = {
    "host": "db",  # Service name from docker-compose.yaml
    "port": "5432",
    "dbname": "energydb",
    "user": "roboticuser",
    "password": "Hakagami2909@@@",
}

CLIENT_ID = os.getenv("RTE_API_CLIENT_ID")
CLIENT_SECRET = os.getenv("RTE_API_CLIENT_SECRET")

def get_access_token():
    url = "https://digital.iservices.rte-france.com/token/oauth/"
    data = {"grant_type": "client_credentials"}
    response = requests.post(url, data=data, auth=(CLIENT_ID, CLIENT_SECRET))
    return response.json()["access_token"]


def fetch_annual_forecast(token, start_date, end_date):
    # Do not encode the date, use it as-is
    #base_url = os.getenv("RTE_API_BASE_URL")
    base_url = "https://digital.iservices.rte-france.com"
    print("Base URL:", base_url)  # Debug: print the base URL
    url = (
        f"{base_url}/open_api/consumption/v1/annual_forecasts?start_date={start_date}&end_date={end_date}"
    )
    print("Request URL:", url)  # Debug: print the actual URL sent to the API
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print("Error fetching annual forecast:", response.status_code, response.text)
        raise Exception("Failed to fetch annual forecast from RTE API")
    try:
        return response.json()
    except Exception as e:
        print("JSON decode error:", e)
        print("Response text:", response.text)
        raise


def save_annual_to_db(data):
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    # Create tables if they do not exist
    cur.execute("""
    CREATE TABLE IF NOT EXISTS energy_annual (
        start_date VARCHAR PRIMARY KEY,
        end_date VARCHAR
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS energy_annual_values (
        start_date VARCHAR,
        end_date VARCHAR,
        average_load_saturday_to_friday INTEGER,
        average_load_monday_to_sunday INTEGER,
        weekly_minimum INTEGER,
        weekly_maximum INTEGER,
        average_load_updated_date VARCHAR,
        margin_updated_date VARCHAR,
        forecast_margin INTEGER,
        PRIMARY KEY (start_date, end_date)
    );
    """)

    for year in data["annual_forecasts"]:
        # Save year info
        cur.execute(
            """
            INSERT INTO energy_annual (start_date, end_date)
            VALUES (%s, %s)
            ON CONFLICT (start_date) DO NOTHING;
            """,
            (
                year["start_date"],
                year["end_date"],
            )
        )
        # Save each weekly value for the year
        for v in year["values"]:
            cur.execute(
                """
                INSERT INTO energy_annual_values (
                    start_date, end_date, average_load_saturday_to_friday, average_load_monday_to_sunday,
                    weekly_minimum, weekly_maximum, average_load_updated_date, margin_updated_date, forecast_margin
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (start_date, end_date) DO NOTHING;
                """,
                (
                    v["start_date"],
                    v["end_date"],
                    v["average_load_saturday_to_friday"],
                    v["average_load_monday_to_sunday"],
                    v["weekly_minimum"],
                    v["weekly_maximum"],
                    v["average_load_updated_date"],
                    v["margin_updated_date"],
                    v["forecast_margin"],
                )
            )

    conn.commit()
    print("Annual forecast data saved to database successfully.")
    cur.close()
    conn.close()

def delete_annual_tables():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS energy_annual_values;")
    cur.execute("DROP TABLE IF EXISTS energy_annual;")
    conn.commit()
    print("Annual tables deleted if they existed.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    token = get_access_token()
    # Fetch and save annual forecast data for each year from 2015 to 2025
    delete_annual_tables()  # Optional: Uncomment if you want to reset tables each run
    for year in range(2015, 2026):
        start_date = f"{year}-01-01T00:00:00+01:00"
        end_date = f"{year+1}-01-01T00:00:00+01:00"
        print(f"Fetching and saving data for {year}...")
        data = fetch_annual_forecast(token, start_date, end_date)
        save_annual_to_db(data)
        print(f"Done for {year}.")

import os
import psycopg2
import unittest
from pipelines.main import save_annual_to_db

def get_test_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

class TestAnnualSave(unittest.TestCase):
    def setUp(self):
        self.conn = get_test_db_connection()
        self.cur = self.conn.cursor()
        # Clean up tables before test
        self.cur.execute("DELETE FROM energy_annual_values;")
        self.cur.execute("DELETE FROM energy_annual;")
        self.conn.commit()

    def tearDown(self):
        self.cur.close()
        self.conn.close()

    def test_save_annual_to_db(self):
        # Example minimal test data
        test_data = {
            "annual_forecasts": [
                {
                    "start_date": "2015-01-01T00:00:00+01:00",
                    "end_date": "2015-12-31T00:00:00+01:00",
                    "values": [
                        {
                            "start_date": "2014-12-29T00:00:00+01:00",
                            "end_date": "2015-01-05T00:30:00+01:00",
                            "average_load_saturday_to_friday": 66300,
                            "average_load_monday_to_sunday": 68800,
                            "weekly_minimum": 55900,
                            "weekly_maximum": 76693,
                            "average_load_updated_date": "2015-01-05T00:30:00+01:00",
                            "margin_updated_date": "2015-01-05T00:30:00+01:00",
                            "forecast_margin": -4300
                        }
                    ]
                }
            ]
        }
        save_annual_to_db(test_data)
        # Check if data was saved
        self.cur.execute("SELECT * FROM energy_annual;")
        annual_rows = self.cur.fetchall()
        self.cur.execute("SELECT * FROM energy_annual_values;")
        value_rows = self.cur.fetchall()
        print("Annual rows:", annual_rows)
        print("Annual value rows:", value_rows)
        self.assertTrue(len(annual_rows) > 0)
        self.assertTrue(len(value_rows) > 0)

if __name__ == "__main__":
    unittest.main()

import requests
import os
from dateutil.relativedelta import relativedelta
from datetime import datetime
from airflow.models import Variable

SECRET_API_APP_ID = Variable.get("SECRET_API_APP_ID")
SECRET_API_APP_KEY = Variable.get("SECRET_API_APP_KEY")


def extract_data_from_api(ti, execution_date) -> None:
    """Extracts flight data from Amsterdam Airport Schiphol API based on date

    Args:
        ti: Airflow's task instance
        execution_date: Airflow execution date
    """
    today = datetime.strftime(execution_date, "%Y-%m-%d")
    tomorrow = datetime.strftime((execution_date + relativedelta(days=1)), "%Y-%m-%d")

    headers = {
        "accept": "application/json",
        "resourceversion": "v4",
        "app_id": SECRET_API_APP_ID,
        "app_key": SECRET_API_APP_KEY,
    }

    all_flights = []
    page = 1
    while True:
        url = f"https://api.schiphol.nl/public-flights/flights?scheduleDate={today}&includedelays=false&page={page}&toScheduleDate={tomorrow}"
        response = requests.request("GET", url, headers=headers)
        if response.status_code == 200:
            flights = response.json()
            if not flights["flights"]:
                break
            else:
                for flight in flights["flights"]:
                    all_flights.append(flight)
                page += 1
        else:
            print(
                f"Something went wrong, Http response code: {response.status_code} - {response.text}"
            )
            break

    ti.xcom_push("flights_raw", {today: all_flights})

import json
import os
import pymongo
from datetime import datetime
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.exceptions import AirflowFailException
from airflow.models import Variable

MONGO_DB_USER = Variable.get("MONGO_DB_USER")
MONGO_DB_PASSWORD = Variable.get("MONGO_DB_PASSWORD")

MONGO_DB_DATABSE = "flights"
MONGO_DB_COLLECTION = "flight_raw"


def _create_connection_with_collection():
    """Creates connection woth MongoDB collection

    Returns:
        conncetion with MongoDB collection
    """
    url = pymongo.MongoClient(
        f"mongodb+srv://{MONGO_DB_USER}:{MONGO_DB_PASSWORD}@cluster0.b52spga.mongodb.net/"
    )
    
    db = url[MONGO_DB_DATABSE]
    collection = db[MONGO_DB_COLLECTION]
    return collection


def upload_to_mongo(ti, **context) -> None:
    """Uploads data from Airflow Xcom to MongoDB

    Args:
        ti: Airflow's task instance

    Raises:
        AirflowFailException: Exception is raised when connection with MongoDB fails
    """
    # FIXME: use Hook
    # try:
    #     hook = MongoHook(mongo_conn_id='mongo_default')
    #     client = hook.get_conn()
    #     # db = client.flights
    #     # currency_collection=db.currency_collection
    #     print(f"Connected to MongoDB - {client}")
    #     # d=json.loads(context["result"])
    #     # currency_collection.insert_one(d)
    # except Exception as e:
    #     print(f"Error connecting to MongoDB -- {e}")

    try:
        collection = _create_connection_with_collection()
        data = ti.xcom_pull(key="flights_raw")
        today = list(data.keys())[0]
        data = {"_id": today, "data": data[today]}
        collection.save(data)
    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")
        raise AirflowFailException


def get_data_from_mongo(execution_date) -> None:
    """Retrieve data from MongoDB based on execution_date

    Args:
        execution_date: Airflow execution date
    """
    today = datetime.strftime(execution_date, "%Y-%m-%d")

    collection = _create_connection_with_collection()
    results = []
    for document in collection.find({}):
        if document["_id"] == today:
            for flight in document["data"]:
                results.append(flight)

    if not results:
        print(f"Didn't find any data based on execution date {today}")
        raise AirflowFailException

    return results

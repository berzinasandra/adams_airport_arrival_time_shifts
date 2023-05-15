import pymongo
from datetime import datetime
from airflow.exceptions import AirflowFailException
from airflow.models import Variable

SECRET_MONGO_DB_USER = Variable.get("SECRET_MONGO_DB_USER")
SECRET_MONGO_DB_PASSWORD = Variable.get("SECRET_MONGO_DB_PASSWORD")

MONGO_DB_DATABSE = "flights"
MONGO_DB_COLLECTION = "flight_raw"

def _create_connection_with_collection():
    """Creates connection woth MongoDB collection

    Returns:
        conncetion with MongoDB collection
    """
    url = pymongo.MongoClient(
        f"mongodb+srv://{SECRET_MONGO_DB_USER}:{SECRET_MONGO_DB_PASSWORD}@cluster0.b52spga.mongodb.net/"
    )
    
    db = url[MONGO_DB_DATABSE]
    collection = db[MONGO_DB_COLLECTION]
    return collection


def upload_to_mongo(ti) -> None:
    """Uploads data from Airflow Xcom to MongoDB

    Args:
        ti: Airflow's task instance

    Raises:
        AirflowFailException: Exception is raised when connection with MongoDB fails
    """
    try:
        collection = _create_connection_with_collection()
        data = ti.xcom_pull(key="flights_raw")
        if not data:
            print(f"NO data was picked up from Xcom")
            raise AirflowFailException
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

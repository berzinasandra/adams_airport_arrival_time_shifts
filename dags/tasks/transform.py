import pandas as pd
from tasks.mongo_db import get_data_from_mongo
from tasks.azure_blob import save_to_blob_storage


def _format_all_flight_data(data: list) -> pd.DataFrame:
    """Formats all flight data

    Args:
        data (list): All data points on all flights

    Returns:
        pd.DataFrame: Selected data points on all flights
    """
    all_flights = []
    for flight in data:
        all_flights.append(
            {
                "id": flight["id"],
                "flight_name": flight["flightName"],
                "direction": flight["flightDirection"],
                "estimated_landing_time": flight["estimatedLandingTime"],
                "actual_landing_time": flight["actualLandingTime"],
                "destination": flight["route"]["destinations"],
            }
        )

    all_flights_df = pd.DataFrame(
        all_flights,
        columns=[
            "id",
            "flight_name",
            "direction",
            "estimated_landing_time",
            "actual_landing_time",
            "destination",
        ],
    )
    return all_flights_df


def _extract_arrival_data(all_flights_df: pd.DataFrame) -> pd.DataFrame:
    """Extracts arrival data from all flights

    Args:
        all_flights_df (pd.DataFrame): Data on all flights

    Returns:
        pd.DataFrame: DataFrame on Arrivals info
    """

    arivals_df = all_flights_df.loc[(all_flights_df["direction"] == "A")]
    return arivals_df

def _format_to_datetime(df:pd.DataFrame, column_name:str) -> pd.DataFrame:
    """Formats column to datetime dtype

    Args:
        df (pd.DataFrame): DataFrame with arrivals data
        column_name (str): Column name

    Returns:
        pd.DataFrame: formatted DataFrame
    """
    df[column_name] = pd.to_datetime(df[column_name])
    return df

def _format_time_shift_columns(arivals_df:pd.DataFrame, column_name:str) -> pd.DataFrame:
    """Format time shift columns into minutes

    Args:
        arivals_df (pd.DataFrame): DataFrame with arrivals data
        column_name (str): Column name

    Returns:
        pd.DataFrame: formatted DataFrame
    """
    arivals_df[column_name] = pd.to_timedelta(arivals_df[column_name])
    arivals_df[column_name] = (
        arivals_df[column_name].dt.total_seconds() / 60
    ).round()

    return arivals_df


def _create_arrival_time_shift_data(arivals_df: pd.DataFrame) -> pd.DataFrame:
    """Calculates arrival time shift

    Args:
        arivals_df (pd.DataFrame): DataFrame on arrival data

    Returns:
        pd.DataFrame:  DataFrame about Arrival time shift
    """
    arivals_df = _format_to_datetime(arivals_df, "actual_landing_time")
    arivals_df = _format_to_datetime(arivals_df, "estimated_landing_time")
    

    arivals_df.loc[
        arivals_df["actual_landing_time"] > arivals_df["estimated_landing_time"],
        ["late_minutes"],
    ] = (
        arivals_df["actual_landing_time"] - arivals_df["estimated_landing_time"]
    ) 
    arivals_df.loc[
        arivals_df["actual_landing_time"] < arivals_df["estimated_landing_time"],
        ["early_minutes"],
    ] = (
        arivals_df["estimated_landing_time"] - arivals_df["actual_landing_time"]
    )

    arivals_df = _format_time_shift_columns(arivals_df, "late_minutes")
    arivals_df = _format_time_shift_columns(arivals_df, "early_minutes")

    return arivals_df


def save_arrival_time_shifts(execution_date) -> None:
    """Extract Arrival time shift data and saves in Blob Storage

    Args:
        execution_date: Airflow execution date
    """
    data = get_data_from_mongo(execution_date)
    all_flights_df = _format_all_flight_data(data)
    arivals_df = _extract_arrival_data(all_flights_df)
    arrival_time_shift_df = _create_arrival_time_shift_data(arivals_df)
    save_to_blob_storage(arrival_time_shift_df, execution_date)

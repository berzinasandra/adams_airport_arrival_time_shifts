import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import io
from datetime import datetime

from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

AZURE_CONN = WasbHook(wasb_conn_id="azure_blob")
CONTAINER_BLOB = "flight-data"


def _create_byetes_file(df: pd.DataFrame) -> io.BytesIO:
    """Transforms Pandas DataFrame into Bytes file

    Args:
        df (pd.DataFrame): Pandas DataFrames

    Returns:
        io.BytesIO: Arrival data as Bytes file
    """
    arrival_data = io.BytesIO()
    pa_table = pa.Table.from_pandas(df)
    writer = pq.ParquetWriter(arrival_data, pa_table.schema)
    writer.write_table(pa_table)
    return arrival_data


def save_to_blob_storage(df: pd.DataFrame, execution_date) -> None:
    """Uploads DataFrame data to Azure Blob Storage

    Args:
        df : Pandas DataFrames
        execution_date: Airflow execution date
    """
    arrival_data = _create_byetes_file(df)
    today = datetime.strftime(execution_date, "%Y_%m_%d")
    AZURE_CONN.upload(
        CONTAINER_BLOB,
        f"{today}_arrivals_time_shift.parquet",
        arrival_data.getvalue(),
        overwrite=True,
    )

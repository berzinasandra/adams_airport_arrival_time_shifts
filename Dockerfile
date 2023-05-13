FROM apache/airflow:2.2.3

ENV PYTHONPATH "/opt/airflow/"

COPY requirements.txt .
RUN pip install pip --upgrade
RUN pip install --no-cache-dir -r requirements.txt

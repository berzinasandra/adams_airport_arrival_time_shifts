#!/bin/sh
FROM apache/airflow:2.2.4-python3.9

ENV PYTHONPATH "/opt/airflow/"

COPY encrypted_variables.yml .
COPY encrypted_connections.yml . 
COPY vault_password.txt .
COPY entrypoint.sh .

COPY requirements.txt .
RUN pip install pip --upgrade
RUN pip install --no-cache-dir -r requirements.txt

USER root
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]

# Amsterdam Airport Schiphol arrival time shifts

## The aim:
The pipeline is built to extract from Airport Schiphol [API](https://www.schiphol.nl/en/developer-center/) flight data on a daily basis and extract arrival flights and present arrival flight time shifts - arriving earlier or later.

## A structure of the pipeline:
The pipeline is built using Airflow and runs daily.
- Collects flight data from Airport Schiphol API 
- Stores raw data in MongoDB
- Retrieves data from MongoDB collection
- Transforms data and extracts only arrivals data
- Based on Estimated arrival time and Actual arrival time calculates the flight's time shift
- Store results in Azure Blob Storage

## To run pipeline locally:
You will need [Docker](https://www.docker.com/).
To start up Airflow run ```docker compose build``` and  ```docker compose up```.
When airflow has booted up, you can open its interface by navigating to 
<http://localhost:8080> in your web browser. Username and password is ```airflow```  

# Airflow Data Pipeline Project

This project demonstrates how to build an Apache Airflow Directed Acyclic Graph (DAG) to orchestrate a data workflow. The workflow includes loading a CSV file, interacting with the OpenWeatherMap API, merging the data, loading it into a local database, and sending email notifications.

---

## Workflow Overview

The Airflow DAG performs the following steps:

1. **Load Data**: Reads the `cities.csv` file containing city details such as name, country, and population.
2. **Call API**: Fetches current weather data for the listed cities from the OpenWeatherMap API.
3. **Merge Data**: Combines the CSV data with the API response based on the city name.
4. **Load Data into Database**: Stores the merged data into a local SQLite database for further analysis.
5. **Clean-Up**: Cleans up temporary resources such as file handlers and database connections.
6. **Email Notification**: Sends an email notification with the status of the pipeline (success or failure).

---

## Prerequisites

- **Apache Airflow**: Ensure you have Apache Airflow installed and set up.
- **Python Dependencies**: Install required Python packages using:
  ```bash
  pip install -r requirements.txt

OpenWeatherMap API Key: Obtain an API key from OpenWeatherMap and update the config/config.yaml file.

#!/bin/bash
# Set AIRFLOW_HOME environment variable to housing_project directory
export AIRFLOW_HOME=housing_project

# Initialize the Airflow database
airflow initdb

# Start the Airflow scheduler in background
airflow scheduler -D &

# Start the Airflow webserver in foreground (to keep container running)
airflow webserver -D

# start airflow as a daemon/background process
# airflow kerberos -D
# airflow scheduler -D
# airflow webserver -D

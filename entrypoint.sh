#!/bin/bash
# Start the Airflow scheduler in background
airflow scheduler &

# Start the Airflow webserver in foreground (to keep container running)
airflow webserver
#!/bin/bash
# Start the Airflow scheduler in background
airflow scheduler -D &

# Start the Airflow webserver in foreground (to keep container running)
airflow webserver -D

# start airflow as a daemon/background process
# airflow kerberos -D
# airflow scheduler -D
# airflow webserver -D

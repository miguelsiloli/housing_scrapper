# Use an official Python runtime as a parent image, based on Debian
FROM python:3.10.6-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Initialize the database for Airflow
RUN airflow db init

# Expose ports (Airflow webserver on 8080)
EXPOSE 8080

# Run your application when the container launches
# CMD ["python", "housing_project/parser.py"]

# Copy the entrypoint script into the container
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set the default command to execute
CMD ["/entrypoint.sh"]

# docker build -t my-airflow-app .
# docker run -d -p 8080:8080 --name my-airflow-instance my-airflow-app

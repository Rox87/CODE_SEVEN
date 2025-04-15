FROM apache/airflow:2.5.1

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow

# Create Airflow directories
RUN mkdir -p ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/plugins

# Set permissions for Airflow directories
RUN chown -R airflow: ${AIRFLOW_HOME}

# Expose ports for the webserver
EXPOSE 8080

# Copy Requirements file
COPY requirements.txt requirements.txt

# Install additional Python packages if needed
RUN pip install --no-cache-dir -r requirements.txt

# Copy DAG file and helper modules
COPY seven_airflow_etl_dag.py ${AIRFLOW_HOME}/dags/

RUN airflow db init

# Add Airflow user
RUN airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com


# Default entrypoint to start Airflow
ENTRYPOINT ["bash", "-c", "airflow db init && airflow scheduler & airflow webserver"]
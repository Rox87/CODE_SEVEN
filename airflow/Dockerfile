FROM apache/airflow:2.5.1

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow

# Create Airflow directories
RUN mkdir -p ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/plugins

# Set permissions for Airflow directories
RUN chown -R airflow: ${AIRFLOW_HOME}

# Expose ports for the webserver
EXPOSE 8080

# Install additional Python packages if needed
RUN pip install --no-cache-dir pandas numpy

RUN airflow db init
# Default entrypoint to start Airflow
ENTRYPOINT ["bash", "-c", "airflow db init && airflow scheduler & airflow webserver"]

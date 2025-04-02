FROM apache/airflow

# Switch to root to install packages
USER root

# Install git
RUN apt-get update && apt-get install -y git && apt-get clean

# Creating empty folder if not exist, the folder ignored / not been created because of .gitignore
RUN mkdir -p /opt/airflow/logs/scheduler /opt/airflow/logs/worker /opt/airflow/logs/webserver /opt/airflow/lib/data
RUN chmod -R 777 /opt/airflow/logs /opt/airflow/lib/data

# Switch back to airflow user
USER airflow

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt

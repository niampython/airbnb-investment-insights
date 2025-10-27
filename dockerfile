# Start from the Apache Airflow official image
FROM apache/airflow:2.9.3-python3.11

# Switch to root to install system dependencies
USER root

# Install Microsoft SQL ODBC drivers and tools
RUN apt-get update && \
    apt-get install -y curl gnupg apt-transport-https unixodbc-dev && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/12/prod.list -o /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 mssql-tools18 && \
    echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user BEFORE installing Python packages
USER airflow

# Copy your Python requirements file
COPY --chown=airflow:airflow requirements.txt .

# Install Python dependencies as airflow user
RUN pip install --no-cache-dir -r requirements.txt



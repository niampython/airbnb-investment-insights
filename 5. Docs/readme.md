# 🧠 USA Airbnb Data Engineering Project — Azure SQL + Airflow ETL

## 🌍 Project Overview
This project demonstrates a **complete cloud-based data engineering pipeline** that automates the extraction, transformation, and loading (ETL) of **Airbnb property data** from the **AirDNA API** into **Azure SQL Database**.

The workflow includes:

1. **Migration** — Using **Azure Data Migration Service (DMS)** to move existing data from an on-premises SQL Server into Azure SQL Database.
2. **Database Configuration** — Altering roles (such as `db_owner`) to grant access and enable ETL connections.
3. **Pipeline Development** — Building a **Python ETL script** for data extraction, transformation, and loading.
4. **Workflow Orchestration** — Deploying the ETL using **Apache Airflow (Dockerized)** for full automation.
5. **Logging & Monitoring** — Implementing detailed logging with Loguru for tracking ETL runs.
6. **Version Control** — Managing code and configurations in GitHub with Docker integration for reproducibility.

---

## 🧱 Project Architecture

```mermaid
graph TD
    A[On-Prem SQL Server] -->|Azure DMS| B[Azure SQL Database]
    B -->|Connected via SQLAlchemy| C[Python ETL Pipeline]
    C -->|Automated by| D[Airflow (Docker)]
    D -->|Logs to| E[Loguru / Local Logs]
    C -->|Consumes Data from| F[AirDNA API (RapidAPI)]

⚙️ Tech Stack

| Layer                        | Tools / Technologies                        |
| ---------------------------- | ------------------------------------------- |
| **Cloud Platform**           | Microsoft Azure                             |
| **Database**                 | Azure SQL Database                          |
| **Migration**                | Azure Data Migration Service                |
| **Orchestration**            | Apache Airflow (Dockerized)                 |
| **Language**                 | Python 3.11                                 |
| **Logging**                  | Loguru                                      |
| **API Integration**          | AirDNA API via RapidAPI                     |
| **Libraries**                | pandas, numpy, requests, sqlalchemy, pyodbc |
| **Secrets Management**       | python-dotenv (.env)                        |
| **Containerization**         | Docker, Docker Compose                      |
| **Version Control**          | Git, GitHub                                 |
| **Visualization (optional)** | Power BI / Tableau connected to Azure SQL   |


🗄️ Step 1: Database Setup
1️⃣ Create Azure SQL Database

In the Azure Portal → SQL Databases → Create new database.

Configure resource group, region, compute tier, and SQL server.

2️⃣ Migrate On-Prem SQL Data

Use Azure Data Migration Service (DMS) to move existing tables/data from your local SQL Server.

The DMS wizard helps with schema and data movement seamlessly.

Verify row counts and integrity post-migration.

3️⃣ Alter Roles for ETL Access

Once your Azure SQL DB is live, assign permissions to your ETL service account:

ALTER ROLE db_owner ADD MEMBER [your_username];

This ensures the pipeline can read/write tables in Azure SQL.


🐍 Step 2: Python ETL Pipeline
Overview

The Python ETL pipeline (scripts/airdna_etl_pipeline.py) automates:

Extract: Fetching Airbnb listings and properties for sale via the AirDNA API.

Transform: Cleaning, renaming, normalizing columns (with Pandas).

Load: Inserting structured data into Azure SQL Database.

ETL Functions
Function	Description
extract1()	Extracts Airbnb active listings for all major U.S. cities
transform1()	Cleans and standardizes the listings dataset
extract2()	Extracts for-sale Airbnb investment properties
transform2()	Normalizes nested JSON from the API and renames fields
load()	Loads both datasets to Azure SQL (Airbnb_Listings and Airbnb_ForSale_Properties)
Logging

Each function writes logs to:

logs/info/info.log
logs/error/error.log
logs/debug/debug.log
logs/warning/warning.log


Loguru automatically timestamps, tags, and filters logs by level.

🧩 Step 3: Airflow DAG (Automation)
File: dags/airdna_etl_dag.py

The Airflow DAG orchestrates the Python ETL tasks as scheduled jobs.

Task ID	Description	Output
extract1	Fetches Airbnb active listings from API	Raw JSON
transform1	Cleans and transforms active listings	Transformed DataFrame
extract2	Fetches for-sale Airbnb property data	Raw JSON
transform2	Cleans for-sale property data	Transformed DataFrame
load	Loads both cleaned datasets into Azure SQL	✅ Data in cloud
DAG Configuration
@dag(
    dag_id="airdna_etl_dag",
    description="Full Airdna ETL Pipeline with extraction, transformation, and Azure SQL load",
    schedule_interval="@monthly",
    start_date=datetime(2025, 10, 21),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["airdna", "etl", "azure", "full"]
)


Schedule: Monthly

Retries: 1

Tags: airdna, etl, azure

Start Date: October 2025

🐳 Step 4: Dockerized Airflow Environment

Your Airflow setup runs fully inside Docker for reproducibility and portability.

📘 Dockerfile — Defines the environment

The Dockerfile is a blueprint that tells Docker how to build the image.

Typical content:

FROM apache/airflow:2.9.2-python3.11

# Copy project files
COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt


✅ Purpose:

Starts from an Airflow base image.

Installs all dependencies (pandas, loguru, requests, sqlalchemy, pyodbc).

Copies your DAGs and scripts into the Airflow container.

📘 docker-compose.yaml — Runs all services together

The docker-compose.yaml defines all containers that work together (Airflow webserver, scheduler, Postgres, Redis, etc.).

Typical structure:

version: '3'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - ./pg_data:/var/lib/postgresql/data

  redis:
    image: redis:latest

  airflow-webserver:
    image: airflow-etl:latest
    restart: always
    depends_on: [postgres, redis]
    ports:
      - "8080:8080"
    environment:
      - LOAD_EX=n
      - EXECUTOR=Celery

  airflow-scheduler:
    image: airflow-etl:latest
    restart: always
    depends_on: [airflow-webserver]


✅ Purpose:

Runs all Airflow components in isolated containers.

Enables one-command startup:

docker-compose up -d


Accessible at: http://localhost:8080

📘 .gitignore — Prevents sensitive or unnecessary files from being tracked

The .gitignore ensures your Git repository stays clean and secure.

Typical content:

# Environment files
.env
__pycache__/
*.pyc

# Logs
logs/
*.log

# Docker
*.pid
*.db
.dockerignore

# IDE
.vscode/
.idea/


✅ Purpose:

Protects secrets (.env) from exposure.

Excludes auto-generated or system files.

Keeps your repo lightweight.

📘 requirements.txt — Lists all dependencies

This file tells Docker and other users which Python packages your project needs.

Example:

pandas
numpy
requests
sqlalchemy
pyodbc
loguru
python-dotenv
apache-airflow==2.9.2


✅ Purpose:

Ensures consistent environment setup.

Used in both local and Docker builds:

pip install -r requirements.txt

🧪 Step 5: Running the Pipeline
Option 1 — Run manually (local environment)
conda activate airflow-env
python scripts/airdna_etl_pipeline.py

Option 2 — Run in Airflow (Docker)
docker-compose up -d


Then open:

http://localhost:8080


Login: airflow / airflow

Enable DAG → “airdna_etl_dag”

Click Trigger DAG

📊 Data Outputs in Azure SQL
Table 1 — Airbnb_Listings
Column	Description
airbnb_property_id	Unique property ID
City	Property city
State	Property state
average_daily_rate	Average LTM daily rate
occupancy_rate	Historical occupancy
revenue	LTM revenue (USD)
Table 2 — Airbnb_ForSale_Properties
Column	Description
Property_id	Unique listing ID
City	City where property is listed
Estimated_Yield	Projected ROI
List_Price	Asking price
Estimated_Revenue	Projected Airbnb revenue
🔐 .env File (Secrets Management)

Example:

API_KEY=your_rapidapi_key
server=yourserver.database.windows.net
database=Airbnb_DB
username=your_username
password=your_password


⚠️ Important: Never commit .env to GitHub. Add it to .gitignore.

🚀 Future Enhancements

Integrate dbt for SQL transformations inside Azure.

Add Power BI dashboards for revenue & occupancy visualization.

Implement Airflow XCom for better task data passing.

Add retry and exponential backoff for API rate limits.

Containerize with Azure Container Instances or AKS.
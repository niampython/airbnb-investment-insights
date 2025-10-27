# 🧭 Data Flow Explanation

## Overview
This document describes the end-to-end data flow for the **Azure Data Engineering Project**.  
The solution contains **two major data pipelines**:

1. **Airdna ETL Pipeline** – automated by **Apache Airflow** using Python scripts to extract, transform, and load (ETL) data into **Azure SQL Database**.  
2. **On-Prem SQL Server Migration** – a one-time (or scheduled) transfer of an existing on-premise SQL Server table to **Azure SQL Database** using **Azure Database Migration Service (DMS)**.

---

## 🧩 1. Airdna ETL Pipeline (API → Azure SQL)

### **1.1 Source**
- **System:** Airdna API  
- **Data:** Short-term rental property listings, occupancy, revenue, and pricing metrics.  
- **Format:** JSON responses fetched via REST API.

### **1.2 Orchestration: Apache Airflow**
- Airflow acts as the **central control layer** for the pipeline.  
- A **DAG (Directed Acyclic Graph)** defines the ETL workflow consisting of:
  - `extract_airdna_data`
  - `transform_airdna_data`
  - `load_to_azure_sql`
- Airflow schedules the DAG to run automatically (e.g., daily or weekly).
- Each task is implemented as a Python operator.

**Control Flow:**


---

### **1.3 Extraction Phase**
- **Tool:** Python script (`extract_api_data.py`)
- **Process:**
  - Makes authenticated API calls to the Airdna REST endpoint.
  - Handles pagination and rate limits.
  - Extracts relevant fields (property_id, location, revenue, occupancy_rate, etc.).
  - Writes raw data to temporary storage or directly into a Pandas DataFrame for processing.

**Output:** Raw JSON → Clean DataFrame

---

### **1.4 Transformation Phase**
- **Tool:** Python script (`transform_data.py`)
- **Process:**
  - Cleans missing or inconsistent data.
  - Renames and standardizes column names.
  - Converts data types (dates, currency, percentages).
  - Applies business logic (e.g., calculate average daily rate, occupancy buckets).
  - Prepares the dataset schema to match Azure SQL target table.

**Output:** Transformed and validated DataFrame ready for loading.

---

### **1.5 Load Phase**
- **Tool:** Python script (`load_to_azure.py`)
- **Destination:** Azure SQL Database
- **Process:**
  - Establishes connection to Azure SQL via `pyodbc` or `SQLAlchemy`.
  - Creates the destination schema/table if it does not exist.
  - Inserts or upserts transformed data.
  - Commits transaction and logs success/failure status back to Airflow.

**Output:** Fresh Airdna data stored in Azure SQL Database.

---

         ┌──────────────────────┐
         │   Apache Airflow     │
         │ (Orchestration DAG)  │
         └──────────┬───────────┘
                    │
    ┌───────────────┼────────────────┐
    │               │                │
[Extract (Python)] → [Transform (Python)] → [Load (Azure SQL)]



---

## 🗄️ 2. On-Prem SQL Server → Azure SQL Database Migration

### **2.1 Source**
- **System:** On-Premises SQL Server Database  
- **Data:** Historical or legacy tables relevant to the same domain.

### **2.2 Destination**
- **System:** Azure SQL Database  
- **Purpose:** Consolidate all data sources in the cloud for analytics and reporting.

### **2.3 Migration Tool: Azure Database Migration Service (DMS)**
- Used to **migrate schema and data** from on-prem SQL Server to Azure SQL.  
- Supports:
  - Schema comparison and validation
  - Full data migration or continuous sync
  - Minimal downtime (if online migration)

**Process Steps:**
1. Set up Azure Database Migration Service instance in Azure Portal.
2. Provide source (on-prem) and target (Azure SQL) connection details.
3. Select databases and tables to migrate.
4. Validate schema mapping.
5. Run migration job.
6. Monitor progress and verify data integrity in Azure SQL.

---

## 🔄 3. Combined Data Architecture

             ┌────────────────────────────┐
             │      Apache Airflow DAG    │
             │ (ETL Orchestration Layer)  │
             └───────────┬────────────────┘
                         │
      ┌──────────────────┼───────────────────┐
      │                  │                   │
[Airdna API] → [Python Extract] → [Python Transform] → [Azure SQL DB]
↑
[On-Prem SQL Server] ────────┘
(Migrated via Azure DMS)


---

## 📊 4. Data Utilization
Once data resides in **Azure SQL Database**, it can be:
- Queried using SQL for ad-hoc analysis.
- Connected to **Power BI** for visualization and dashboards.
- Integrated into additional analytics or machine learning pipelines.

---

## 🧠 5. Summary

| Step | Technology | Purpose |
|------|-------------|----------|
| Extraction | Python (API calls) | Fetch data from Airdna |
| Transformation | Python (Pandas) | Clean and format data |
| Orchestration | Apache Airflow | Schedule & automate ETL |
| Loading | Azure SQL Database | Store final data |
| Migration | Azure DMS | Move on-prem SQL to Azure SQL |

---

**Author:** Niam Dickerson  
**Last Updated:** _October 2025_

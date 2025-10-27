# ⚙️ Challenges and Solutions

This document outlines the main technical challenges encountered during the implementation of the **Airdna ETL and Anlysis Project**, and the steps taken to resolve them.  
Each issue provided valuable learning experience in cloud networking, authentication, API management, and orchestration tools.

---

## 🧱 1. Connection & Firewall Issues Between On-Prem SQL Server and Azure SQL Database

**Challenge:**  
When migrating data from the on-premises SQL Server to Azure SQL Database using **Azure Database Migration Service (DMS)**, I encountered credential and firewall errors preventing the source from communicating with the target.

**Root Cause:**  
Outbound traffic from the on-prem environment was being blocked by Azure’s default firewall configuration.

**Solution:**  
- Added a **firewall rule** to the **Azure SQL Server** to allow inbound connections from the on-prem server’s public IP address.  
- Confirmed that outbound traffic was allowed from the on-prem environment.  
- Retested connectivity using SQL Server Management Studio (SSMS) before re-running the migration.

✅ **Result:** The migration service successfully transferred schema and data from on-prem SQL Server to Azure SQL Database.

---

## 🌐 2. API Rate Limit Issues During Data Extraction

**Challenge:**  
While extracting data from the **Airdna API**, frequent **HTTP 429 (Too Many Requests)** errors occurred, indicating API rate limits were being hit.

**Root Cause:**  
The Python extraction script was making too many requests per minute, exceeding the API’s allowed threshold.

**Solution:**  
- Implemented **Exponential Backoff and Retry Logic** in the Python script to gradually increase the wait time between retries.  
- Added **request throttling** to control the number of requests made per second.  
- Upgraded to a higher API usage tier to accommodate larger data pulls.  

✅ **Result:** The extraction process became more reliable and stable, reducing rate-limit errors and ensuring full data retrieval.

---

## 🔐 3. Access and Authentication Issues with Azure SQL Database

**Challenge:**  
After creating the Azure SQL Database, I had difficulty connecting through **SQL Server Management Studio (SSMS)** and my Python ETL scripts due to authentication errors.

**Root Cause:**  
I initially used incorrect authentication mode and connection settings, attempting to log in without specifying the target database or correct credentials.

**Solution:**  
- Learned that Azure SQL supports **two authentication types**:
  1. **Microsoft Entra ID (formerly Azure AD)** authentication  
  2. **SQL Server Authentication** (login name and password)
- Switched to **SQL Server Authentication**, which simplified integration with my ETL scripts and Azure Data Migration Service.
- Updated **Connection Properties** in SSMS to specify the target database under *“Connect to database”* instead of defaulting to the *master* database.

✅ **Result:** Successful authentication and seamless integration between SSMS, Airflow, and the Azure SQL Database.

---

## ☁️ 4. Airflow Scheduler and UI Configuration Problems

**Challenge:**  
Apache Airflow’s **scheduler** and **web UI** were not displaying or running DAGs properly after initial setup.

**Root Cause:**  
Airflow services were not correctly configured within Docker containers, and DAGs were not properly recognized by the environment.

**Solution Steps:**
1. Installed **Docker Desktop** and created a **Dockerfile** to build the custom Airflow image.  
2. Installed:
   - Apache Airflow
   - Microsoft SQL ODBC drivers and tools
   - Python dependencies listed in `requirements.txt`
3. Created a **`docker-compose.yaml`** file to run the **webserver** and **scheduler** containers.  
4. Configured **volumes** in Docker Compose to ensure the following folders were accessible inside containers:
   - `/dags`
   - `/scripts`
   - `/logs`
   - `/plugins`
5. Updated the DAG file to correctly define tasks using the `@task` decorator in Airflow, clearly establishing the workflow sequence:

**Author:** _Niam Dickerson_  
**Last Updated:** _October 2025_
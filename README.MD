# **🏠 Airbnb Investment Insights Data Engineering Project**
## **📘 Project Overview**

As a Data Engineer and Analyst, I built an end-to-end data pipeline to help a real estate investor make informed short-term rental investment decisions using Airbnb (Airdna) data.

The investor’s goal was to determine which U.S. city generates the highest Airbnb revenue, and what property characteristics (bedrooms, bathrooms, pricing, and location features) maximize returns.

This project combines data engineering, Azure cloud infrastructure, data orchestration, and data analytics to generate actionable investment insights.

## **🧩 Business Problem**

The investor wanted to know:

Which state generates the most Airbnb revenue.

Which city within that state provides the highest average annual revenue.

What combination of property features (bedrooms, bathrooms, listing types) yields the highest projected return.

Why travelers visit certain destinations and what type of tourism categories attract the most visitors.

## **☁️ Architecture Overview** 
### **🔹 Data Flow**

The following diagram shows how data flows through the system — from source to cloud to analysis.

(ETL pipeline showing Airdna API, Python ETL scripts, Airflow orchestration, and Azure SQL Database)

**Data Flow Summary:**

**Airdna API (Airbnb source data) —** Extracted via Python

**On-Prem SQL Server (Tourism Data) —** Migrated to Azure SQL using Azure Data Migration Service

**Azure SQL Database —** Central data warehouse for Airbnb and tourism data

**Apache Airflow (via Docker) —** Monthly ETL scheduling and monitoring

**SQL Analysis (SSMS) —** Insights and recommendations for the investor

###  **🧱 Data Pipeline Architecture**

<p align="center">
  <img src="5.%20Docs/architecture_diagram.png" width="800">
</p>


## 🧰 **Technologies Used**

| **Category**        | **Tools / Services**                           |
|---------------------|------------------------------------------------|
| **Cloud**           | Azure SQL Database, Azure Data Migration Service |
| **Orchestration**   | Apache Airflow (via Docker)                    |
| **Programming**     | Python                                         |
| **Querying**        | T-SQL                                          |
| **Database**        | SQL Server                                     |
| **Visualization**   | SSMS, Draw.io                                  |
| **Version Control** | GitHub                                         |


## **⚙️ ETL Process**
### **1️⃣ Extraction**

**Source:** Airdna API (Airbnb property and listing data)

**Tool:** Python (requests, json)

**Process:** Fetches JSON data for Airbnb listings across U.S. cities

### **2️⃣ Transformation**

**Tool:** Python

**Process:** Cleans data, maps schema, and validates metrics (revenue, occupancy rate, etc.)

### **3️⃣ Load**

**Destination:** Azure SQL Database

**Tool:** pyodbc via Python

**Schedule:** Monthly using Airflow DAGs

## **🔁 Cloud Migration (On-Prem → Azure SQL)**

Historical tourism trend data stored in an on-prem SQL Server was migrated to Azure SQL Database using Azure Data Migration Service.

**Benefits:**

Built-in High Availability and Disaster Recovery

Reduced Infrastructure and Maintenance Costs

Centralized data for scalable analytics

## **🧠 Data Analysis (SQL Insights)**

Once data was loaded into Azure SQL Database, several ad-hoc SQL queries were run to generate insights for the investor.

### **1️⃣ Identify Top State and City by Average Revenue**

```sql
WITH avg_rev AS(
    SELECT State,
           avg(Revenue) AS AVG_Annual_Revenue,
           DENSE_RANK() OVER(ORDER BY avg(Revenue) DESC) AS State_Rank
    FROM [dbo].[Airbnb_Listings]
    GROUP BY State
)
SELECT DISTINCT a.State,
       city,
       CONCAT('$', FORMAT(AVG_Annual_Revenue, 'N')) AS 'Average Revenue'
FROM avg_rev a
INNER JOIN [dbo].[Airbnb_Listings] l ON a.State = l.State
WHERE State_Rank = 1;
```

**✅ Result: Virginia → Virginia Beach had the highest average annual Airbnb revenue.**

### **2️⃣ Analyze Average Occupancy, Rating, Daily Rate, and Availability**
```sql
SELECT CONCAT(AVG(CAST([Occupancy_Rate] AS SMALLINT)),'%') AS 'Average Occupancy Rate',
       AVG(CAST(Rating AS SMALLINT)) AS 'Average Rating',
       CONCAT('$', AVG(CAST(Average_Daily_Rate AS SMALLINT))) AS 'Average Daily Rate',
       AVG(CAST(Days_Available AS SMALLINT)) AS 'Average Days Available'
FROM [dbo].[Airbnb_Listings]
WHERE City = 'Virginia Beach'
  AND (Title LIKE '%beach%' OR Title LIKE '%water%' OR Title LIKE '%ocean%');
```

**✅ Insight:**

Average Occupancy Rate: **~60%**

Average Daily Rate: **~$470**

Beachfront and waterfront listings outperform others.

### **3️⃣ Determine Property Types with the Highest Projected Revenue**
```sql
SELECT TOP 5 Listing_Type,
       Bedrooms,
       Bathrooms,
       CONCAT('$', AVG((365 - Days_Available) * CAST(Average_Daily_Rate AS SMALLINT))) AS 'Projected Revenue'
FROM [dbo].[Airbnb_Listings]
WHERE Bedrooms <> 0
  AND City = 'Virginia Beach'
GROUP BY Listing_Type, Bedrooms, Bathrooms
ORDER BY AVG((365 - Days_Available) * Average_Daily_Rate) DESC;
```

**✅ Recommendation:**

**5–7 bedroom** entire homes / apartments perform best

Properties near **beach** or **urban attractions** have the highest ROI

### **4️⃣ Understand Tourism Trends in the U.S.**
```sql
SELECT Country, Historical, Urban, Adventure, Nature, Cultural, Beach
FROM (
   SELECT Country, Category, Visitors
   FROM [dbo].[Tourism_by_Category]
   WHERE Country = 'USA'
) SourceTable
PIVOT
(
    SUM(Visitors)
    FOR Category IN (Historical, Urban, Adventure, Nature, Cultural, Beach)
) AS PivotTable;
```

**✅ Insight:**
Historical attractions, beaches, and urban activities are the top motivators for visitors to the U.S.
👉 Reinforces the investment case for **Virginia Beach**.

## 💡 **Recommendations to the Investor**

| **Recommendation** | **Description** |
|--------------------|-----------------|
| **Target Market** | Virginia Beach, VA |
| **Property Focus** | Entire home/apartment with **5–7 bedrooms**, **5–7 bathrooms** |
| **Attraction Focus** | Near beach, historical, and urban areas |
| **Expected Metrics** | ~60% occupancy rate, ~$470 daily rate |
| **Tourism Insight** | Beach & Urban attractions drive most U.S. travel |


### **📊 Screenshots**



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

### **🗓️ Scheduling & Monitoring**

Airflow schedules monthly ETL pipelines

Dockerized Airflow includes webserver + scheduler

Real-time monitoring via Airflow UI

### **📈 Results**

Automated Airbnb data ingestion and cloud migration pipeline

Full data integration between Airdna API and Azure SQL Database

Actionable, data-driven recommendations for investment strategy

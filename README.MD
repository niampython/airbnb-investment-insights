🏠 Airbnb Investment Insights Data Engineering Project
📘 Project Overview

As a Data Engineer and Analyst, I built an end-to-end data pipeline to help a real estate investor make informed short-term rental investment decisions using Airbnb (Airdna) data.

The investor’s goal was to determine which U.S. city generates the highest Airbnb revenue, and what property characteristics (bedrooms, bathrooms, pricing, and location features) maximize returns.

This project combines data engineering, Azure cloud infrastructure, data orchestration, and data analytics to generate actionable investment insights.

🧩 Business Problem

The investor wanted to know:

Which state generates the most Airbnb revenue.

Which city within that state provides the highest average annual revenue.

What combination of property features (bedrooms, bathrooms, listing types) yields the highest projected return.

Why travelers visit certain destinations and what type of tourism categories attract the most visitors.

☁️ Architecture Overview
🔹 Data Flow

The following diagram shows how data flows through the system — from source to cloud to analysis.

(ETL pipeline showing Airdna API, Python ETL scripts, Airflow orchestration, and Azure SQL Database)

Data Flow Summary:

Airdna API (Airbnb source data) — Extracted via Python.

On-Prem SQL Server (Tourism Data) — Migrated to Azure SQL using Azure Data Migration Service.

Azure SQL Database — Central data warehouse for Airbnb and tourism data.

Apache Airflow (via Docker) — Monthly ETL scheduling and monitoring.

SQL Analysis (SSMS) — Insights and recommendations for the investor.

⚙️ ETL Process
1️⃣ Extraction

Source: Airdna API (Airbnb property and Listing data).

Tool: Python (requests, json).

Process: Fetches JSON data for Airbnb listings across U.S. cities.

2️⃣ Transformation

Tool: Python.

Process: Cleans data, maps schema, and validates metrics (revenue, occupancy rate, etc.).

3️⃣ Load

Destination: Azure SQL Database.

Tool: pyodbc via Python.

Schedule: Monthly using Airflow DAGs.

🔁 Cloud Migration (On-Prem → Azure SQL)

Historical tourism trend data stored in an on-prem SQL Server was migrated to Azure SQL Database using Azure Data Migration Service.

Benefits:

Built-in High Availability and Disaster Recovery

Reduced Infrastructure and Maintenance Costs

Centralized data for scalable analytics

🧠 Data Analysis (SQL Insights)

Once data was loaded into Azure SQL Database, several ad-hoc SQL queries were run to generate insights for the investor.

1️⃣ Identify Top State and City by Average Revenue
WITH avg_rev AS(
    SELECT State, 
           avg(Revenue) AS AVG_Annual_Revenue,
           DENSE_RANK() OVER(ORDER BY avg(Revenue) DESC) AS State_Rank
    FROM [dbo].[Airbnb_Listings]
    GROUP BY State
)

SELECT DISTINCT 
    a.State,
    city,
    CONCAT('$', FORMAT(AVG_Annual_Revenue, 'N')) AS 'Average Revenue'
FROM avg_rev a
INNER JOIN [dbo].[Airbnb_Listings] l
    ON a.State = l.State
WHERE State_Rank = 1;


✅ Result:
Virginia → Virginia Beach had the highest average annual Airbnb revenue.

2️⃣ Analyze Average Occupancy, Rating, Daily Rate, and Availability
SELECT CONCAT(AVG(CAST([Occupancy_Rate] AS SMALLINT)),'%') AS 'Average Occupancy Rate',
       AVG(CAST(Rating AS SMALLINT)) AS 'Average Rating',
       CONCAT('$', AVG(CAST(Average_Daily_Rate AS SMALLINT))) AS 'Average Daily Rate',
       AVG(CAST(Days_Available AS SMALLINT)) AS 'Average Days Available'
FROM [dbo].[Airbnb_Listings]
WHERE City = 'Virginia Beach'
  AND (Title LIKE '%beach%' 
    OR Title LIKE '%water%' 
    OR Title LIKE '%ocean%');


✅ Insight:

Average Occupancy Rate: ~60%

Average Daily Rate: ~$470

Beachfront and waterfront listings outperform others.

3️⃣ Determine Property Types with the Highest Projected Revenue
SELECT TOP 5 
    Listing_Type,
    Bedrooms,
    Bathrooms,
    CONCAT('$', AVG((365 - Days_Available) * CAST(Average_Daily_Rate AS SMALLINT))) AS 'Projected Revenue'
FROM [dbo].[Airbnb_Listings]
WHERE Bedrooms <> 0 
  AND City = 'Virginia Beach'
GROUP BY Listing_Type, Bedrooms, Bathrooms
ORDER BY AVG((365 - Days_Available) * Average_Daily_Rate) DESC;


✅ Recommendation:

5–7 bedroom entire homes / apartments perform best.

Properties near beach or urban attractions have the highest ROI.

4️⃣ Understand Tourism Trends in the U.S.
SELECT Country, Historical, Urban, Adventure, Nature, Cultural, Beach 
FROM (
    SELECT Country, Category, Visitors
    FROM [dbo].[Tourism_by_Category]
    WHERE Country = 'USA'
) SourceTable
PIVOT (
    SUM(Visitors)
    FOR Category IN (Historical, Urban, Adventure, Nature, Cultural, Beach)
) AS PivotTable;


✅ Insight:
Historical attractions, beaches, and urban activities are the top motivators for visitors to the U.S.
👉 Reinforces the investment case for Virginia Beach.

💡 Recommendations to the Investor
Recommendation	Description
Target Market	Virginia Beach, VA
Property Focus	Entire home/apartment with 5–7 bedrooms, 5–7 bathrooms
Attraction Focus	Near beach, historical, and urban areas
Expected Metrics	~60% occupancy rate, ~$470 daily rate
Tourism Insight	Beach & Urban attractions drive most U.S. travel
📊 Screenshots
🧱 Data Pipeline Architecture

![airbnb-investment-insights](Docs/architecture_diagram.png)

🧮 SQL Analysis in SSMS

(image placeholder)

🧰 Technologies Used
Category	Tools / Services
Cloud	Azure SQL Database, Azure Data Migration Service
Orchestration	Apache Airflow (via Docker)
Programming	Python
Querying	T-SQL
Database	SQL Server
Visualization	SSMS, Draw.io
Version Control	GitHub
🗓️ Scheduling & Monitoring

Airflow schedules monthly ETL pipelines

Dockerized Airflow includes webserver + scheduler

Real-time monitoring via Airflow UI

📈 Results

Automated Airbnb data ingestion and cloud migration pipeline

Full data integration between Airdna API and Azure SQL Database

Actionable, data-driven recommendations for investment strategy

***This guide provides a detailed walkthrough of creating and running a data migration from an on-premises SQL Server database to an Azure SQL Database using Azure Database Migration Service (DMS).***

# ***🧭 1. Prerequisites***

Before starting the migration, ensure the following prerequisites are met:

✅ You already have an on-premises SQL Server with an existing database and tables containing data.

✅ An Azure SQL Database and Azure SQL Server are already created.

✅ You have network connectivity between your on-prem environment and Azure (firewall rules configured).

✅ You have admin credentials for both the on-prem SQL Server and the Azure SQL Database.

✅ You have permissions to create and manage Azure resources (e.g., Owner or Contributor role).

## ***⚙️ 2. Create an Azure Database Migration Service (DMS) Instance***
*** Step 2.1 ***— Go to Azure Portal 
Navigate to https://portal.azure.com

In the search bar, type Database Migration Services.

Click Create.

***Step 2.2 *** — Fill in Required Details

Subscription: Select your active subscription.

Resource group: Choose an existing group or create a new one.

Name: Example — TouristDataMigrationService.

Region: Select the same region as your target Azure SQL Database (for best performance).

Pricing Tier: Choose Standard or Premium depending on the size of your data.

Click Review + Create → Create.

## ***🧩 3. Register an Integration Runtime (if required)***

If you’re migrating from an on-premises SQL Server, DMS needs a self-hosted Integration Runtime (IR) — this acts as a secure bridge between your local server and Azure.

*** Step 3.1 *** — Download and Install Integration Runtime

After your DMS resource is created, open it in the portal.

On the Overview page, look for Integration Runtime State: Not registered.

Click Register Integration Runtime and follow the instructions:

Download the Microsoft Integration Runtime installer.

Install it on the on-premises machine (the one hosting SQL Server or one that has network access to it).

During installation, enter the authentication key shown in the Azure portal to link it to your DMS resource.

Once registered, the status changes to Integration Runtime: Registered.

## ***🧱 4. Create a New Migration Project***
*** Step 4.1 ***— Create a New Project

Go to your DMS resource (e.g., TouristDataMigrationService).

Click + New Migration at the top.

Fill in the following details:

Source type: SQL Server

Target type: Azure SQL Database

Activity type: Offline data migration (data is copied once, and downtime is acceptable)

Click Create and run activity.

## ***🔐 5. Configure Source and Target Connections***
*** Step 5.1 *** — Source Configuration (On-Prem SQL Server)

Enter your on-prem SQL Server details:

Server name:
e.g. ONPREMSERVER01 or localhost\SQLEXPRESS

Authentication type: SQL Server Authentication (recommended for migrations)

Username: e.g. sa or another SQL login

Password: Your SQL password

Encryption: Enabled (recommended)

✅ Click Connect to validate.

*** Step 5.2 *** — Target Configuration (Azure SQL Database)

Enter your Azure SQL Database server details:

Server name:
e.g. touristcategoryserver.database.windows.net

Authentication type: SQL Server Authentication

Username: The admin username you created for Azure SQL Server

Password: The matching password

✅ Click Connect again to validate.

### *** 🧩 6. Select Databases and Tables to Migrate ***

Once both connections are validated:

DMS will display a list of databases available in the source server.

Select the database you want to migrate.

Map it to your target Azure SQL Database.

If your target database already exists, choose it.

If not, DMS can create it automatically.

Review the table mappings (you can choose to migrate specific tables if needed).

### *** 🔄 7. Configure Migration Settings ***

Choose Full Data Migration (copies entire data from source tables).

Optionally, enable Schema Validation and Data Consistency Check.

Review your configurations.

Click Save and Run Migration.

## *** 🕓 8. Monitor the Migration ***

After the migration starts:

Go to your DMS resource → Migrations tab.

Select your migration to open the Migration Project Dashboard.

View:

Migration status (Queued, Running, Completed, Failed)

Source & Target details

Duration and completion time

When the status changes to Completed, verify data in Azure SQL Database.

## *** 🧾 9. Post-Migration Validation ***

After migration:

Open SQL Server Management Studio (SSMS).

Connect to your Azure SQL Database:

Server name: <yourservername>.database.windows.net

Authentication: SQL Server Authentication

Username/Password: Same as your Azure SQL Server credentials

Query the target database:

SELECT COUNT(*) FROM [dbo].[YourTable];


Compare with the on-prem SQL Server table to confirm successful data transfer.

## *** 🔒 10. Security and Credential Management ***

Aspect	Description	Recommendation

🔑 Credentials	SQL Authentication used for both source and target connections	Store credentials securely using Azure Key Vault or local .env files (never in scripts)

🔥 Firewall Rules	Azure SQL Database must allow the DMS service and on-prem IPs	Add IP rules in Azure SQL Server → Networking

🔗 Integration Runtime	Acts as a secure data bridge	Install only on trusted on-prem servers

🔐 Encryption	Ensures secure transmission between on-prem and Azure	Always enable encryption for both source and target

🧰 11. Optional — Troubleshooting Tips

Issue	Possible Cause	Fix

❌ Migration fails to start	Integration Runtime not registered	Go to DMS → Register Integration Runtime

🔒 Login failed for source	Wrong authentication type or credentials	Use SQL Authentication (not Windows)

🔌 Cannot connect to target	Azure SQL firewall blocking connection	Add IP of DMS or Integration Runtime to server-level firewall rules

⚠️ Data mismatch	Partial migration or timeout	Re-run the migration or perform incremental sync

✅ 12. Expected Outcome

Once complete, you’ll have:

A fully migrated Azure SQL Database with all data from your on-prem SQL Server.

Secure, validated connections between your environments.

Logs and migration details visible under Migrations in your DMS dashboard.

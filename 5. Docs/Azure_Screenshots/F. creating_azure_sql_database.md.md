# ***Creating and Connecting to an Azure SQL Database***

This document outlines the full process of creating an Azure SQL Database, configuring firewall rules, enabling SQL Server authentication, and connecting to it using SQL Server Management Studio (SSMS).

## ***🧱 1. Create an Azure SQL Database***
***Step 1.1*** — Go to Azure Portal

Navigate to https://portal.azure.com

Sign in with your Azure credentials.

***Step 1.2*** — Create a New SQL Database

In the search bar at the top, type “SQL Database”.

Click Create → SQL Database.

Fill out the required fields:

Subscription: Select your active subscription.

Resource group: Create a new one (e.g., DataEngineerProject1) or select an existing one.

Database name: Example — TouristCategoryDB.

Server: Click Create new.

## ***⚙️ 2. Create a New SQL Server (Host for the Database)***

When creating a new server:

Enter the following:

Server name: Example — touristcategoryserver.

Server admin login: Example — sqladminuser.

Password: Choose a secure password (save this for later).

Location: Select a region near you (e.g., Central US).

Click OK.

This creates both:

An Azure SQL Database (your actual data storage).

An Azure SQL Server (the logical host for your database).

## ***🔐 3. Configure Firewall Rules to Allow Connections***

By default, Azure SQL blocks all external connections.
You need to allow your local IP address or network to connect.

***Step 3.1*** — Go to Your SQL Server Resource

From the Azure Portal home, go to “SQL servers”.

Click the server you just created (e.g., touristcategoryserver).

***Step 3.2*** — Add a Firewall Rule

In the left menu, click Networking or Firewalls and virtual networks.

Under Firewall rules, click + Add client IP.

This automatically detects your current IP address and adds it.

Click Save at the top.

✅ Your local machine can now connect to the Azure SQL Server.

## ***👤 4. Configure Authentication Method (SQL Authentication)***

When you created the server, you set up a:

Server admin login name

Password

That’s SQL Server Authentication, which uses a username/password instead of Azure AD credentials.

You’ll use these credentials later in SSMS and in your ETL scripts.

## ***💻 5. Connect to Azure SQL Database Using SSMS***
***Step 5.1*** — Open SQL Server Management Studio

Open SSMS on your local machine.

***Step 5.2*** — Connect to Server

When the Connect to Server window appears:

Server type: Database Engine

Server name: <yourservername>.database.windows.net
Example:

touristcategoryserver.database.windows.net


Authentication: SQL Server Authentication

Login: Your admin username (e.g., sqladminuser)

Password: The password you created

## ***💡 Tip: Check “Remember password” if you connect frequently.***

**Step 5.3*** — Connection Properties (Important!)

Click Options >>

Go to the Connection Properties tab.

Under Connect to database, type your actual database name (e.g., TouristCategoryDB).

Click Connect.

✅ You should now be connected to your Azure SQL Database successfully.

## ***🧩 6. Troubleshooting Common Issues***
Issue	Cause	Solution
🔒 Cannot connect / Timeout	Firewall not allowing your IP	Go back to your Azure SQL Server → Networking → Add client IP
❌ Login failed for user	Wrong authentication type	Ensure you’re using SQL Server Authentication (not Windows or Azure AD)
⚠️ Connected to master database	Forgot to specify target DB	In SSMS “Connection Properties” → enter database name manually
🔄 ETL script not connecting	Outbound IP not allowed	Add your VM or script host IP to the Azure SQL firewall rules

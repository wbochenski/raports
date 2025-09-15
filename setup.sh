#!/bin/bash
clear

mkdir raports
mkdir data

# Install necessary packages and set up the SQL Server container
pacman -S --noconfirm unixodbc
sudo -u user yay -S --noconfirm msodbcsql17
docker rm -f mssql-container 2> /dev/null

# Run the SQL Server Docker container
docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=YourStrong.Passw0rd" -p 1433:1433 --name mssql-container -d mcr.microsoft.com/mssql/server:2022-latest

# Wait for the SQL Server to be fully up
echo "Waiting for SQL Server to start..."
sleep 10  # You might want to adjust this time depending on your system's speed

# Python script for interacting with the database
./.venv/bin/python3 <<EOF
import pyodbc
import pandas as pd
import random
from datetime import datetime, timedelta

# Function to generate random date within a range
def random_date(start_date, end_date):
    time_between = end_date - start_date
    days_between = time_between.days
    random_number_of_days = random.randrange(days_between)
    random_datetime = start_date + timedelta(days=random_number_of_days)
    return random_datetime.date()  # Return only the date part

# Connection parameters
server = 'localhost'
database = 'master'
username = 'sa'
password = 'YourStrong.Passw0rd'
driver = '{ODBC Driver 17 for SQL Server}'

try:
    conn = pyodbc.connect(
        f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}',
        autocommit=True  # <<< This is the key change
    )
    print("\033[32mConnected to SQL Server successfully.\033[0m")
    cursor = conn.cursor()

    # Create database
    cursor.execute("CREATE DATABASE Main;")
    print("\033[32mDatabase 'Main' created successfully.\033[0m")

    # Switch to the 'Main' database
    cursor.execute("USE Main;")
    
    # Create the 'Memory' table with an ID, Value, and Date column
    cursor.execute("""
    CREATE TABLE Memory (
        ID NVARCHAR(50) PRIMARY KEY,
        Value NVARCHAR(255),
    );
    """)
    print("\033[32mTable 'Memory' created successfully.\033[0m")

    cursor.execute("""
    CREATE TABLE Data (
        ID NVARCHAR(50) PRIMARY KEY,
        Value INT,
        Date DATE
    );
    """)
    print("\033[32mTable 'Data' created successfully.\033[0m")

    cursor.execute("""
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE';
    """)

    # Fetch and print all table names
    tables = cursor.fetchall()
    if tables:
        print("Tables in database 'Main':")
        for table in tables:
            print(table[0])
    else:
        print("No tables found.")

    # Load the CSV data
    df = pd.read_csv("./data/sample_data.csv", sep=";", names=["ID", "Value"])
    rows = df.values.tolist()

    # Define date range for random date generation
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 12, 31)

    # Insert data with random date
    for i in rows:
        random_dt = random_date(start_date, end_date)
        cursor.execute("INSERT INTO Data (ID, Value, Date) VALUES (?, ?, ?);", (i[0], i[1], random_dt))

    cursor.execute("SELECT * FROM Data;")
    rows = cursor.fetchall()

    # Print each row
    if rows:
        print("Rows in 'Data' table:")
        for row in rows:
            print(f"ID: {row.ID}, Value: {row.Value}, Date: {row.Date}")
    else:
        print("No rows found in 'Data' table.")

    cursor.close()
    conn.close()

except Exception as e:
    print("Operation failed:", e)
EOF

chown -R user:user ./*
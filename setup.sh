#!/bin/bash
clear

# Install necessary packages and set up the SQL Server container
pacman -S --noconfirm docker unixodbc
sudo -u user yay -S --noconfirm msodbcsql17
systemctl start docker
docker rm -f mssql-container 2> /dev/null

# Run the SQL Server Docker container
docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=YourStrong.Passw0rd" -p 1433:1433 --name mssql-container -d mcr.microsoft.com/mssql/server:2022-latest

# Wait for the SQL Server to be fully up
echo "Waiting for SQL Server to start..."
sleep 10  # You might want to adjust this time depending on your system's speed

# Python script for interacting with the database
python3 <<EOF
import pyodbc
import pandas as pd

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
    
    # Create the 'Memory' table with an ID and Value column
    cursor.execute("""
    CREATE TABLE Memory (
        ID NVARCHAR(50) PRIMARY KEY,
        Value NVARCHAR(255)
    );
    """)
    print("\033[32mTable 'Memory' created successfully.\033[0m")

    cursor.execute("""
    CREATE TABLE Data (
        ID NVARCHAR(50) PRIMARY KEY,
        Value INT
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

    df = pd.read_csv("./data/sample_data.csv", sep=";",names=["ID","Value"])
    rows = df.values.tolist()
    for i in rows:
        cursor.execute("INSERT INTO Data (ID, Value) VALUES (?, ?);", (i[0], i[1]))

    cursor.execute("SELECT * FROM Data;")
    rows = cursor.fetchall()

    # Print each row
    if rows:
        print("Rows in 'Data' table:")
        for row in rows:
            print(f"ID: {row.ID}, Value: {row.Value}")
    else:
        print("No rows found in 'Data' table.")

    cursor.close()
    conn.close()

except Exception as e:
    print("Operation failed:", e)
EOF

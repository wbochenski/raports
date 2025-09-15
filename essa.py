from typing import List, Tuple
from translation import *
import pandas as pd
import numpy as np
import pyodbc
import re

logging = True

def GetArguments() -> dict:
    import argparse

    arguments = {}

    parser = argparse.ArgumentParser(description="Process command-line arguments.")

    parser.add_argument('template', type=str, metavar='<template>', help='The path to the template')
    parser.add_argument('raport', type=str, metavar='<raport>', help='The path to save the raport')
    parser.add_argument('--date-from', type=str, help='The starting date parameter', required=True)
    parser.add_argument('--date-to', type=str, help='The ending date parameter', required=True)

    # Parse the arguments
    args = parser.parse_args()

    for i in args._get_kwargs():
        arguments[i[0]] = i[1]
    
    arguments["date_from"] = "'" + arguments["date_from"] + "-01'"
    arguments["date_to"] = "'" + arguments["date_to"] + "-01'"

    return arguments

def ParseTemplateToRaport(db: pyodbc.Cursor, file: dict) -> dict:
    """
    Tries to execute the commands of the third column of the data table

    Args:
        db (pyodbc.Cursor): The cursor object used to execute database queries.
        file (dict): The template file containing 'ID', 'Column Names', and 'Data'.

    Returns:
        dict: A new dictionary containing the parsed template with the updated data.
    """
    Log(f"Parsing template with ID: {file["ID"]}")
    rows = file["Data"].values.tolist()
    for i in range(len(rows)):
        rows[i][2] = ExecuteAnything(db, rows[i][2])

    file2 = {
        "ID": file["ID"],
        "Column Names": file["Column Names"],
        "Data": pd.DataFrame(rows)
    }
    Log(f"Raport parsed.")

    return file2

def ExecuteAnything(cursor: pyodbc.Cursor, query: str) -> List:
    """
    Maps the given query to its corresponding value by executing a translated SQL query.

    Args:
        cursor (pyodbc.Cursor): The cursor object used to execute database queries.
        query (str): The SQL query string to be mapped.

    Returns:
        List: The result of the executed query.
    """
    Log(f"Executing query: {query}")

    if pd.isna(query):
        return query
    
    divided_query = re.split(r'(\s[\+\-\*/]\s)', query)

    Log(f"Query ({query}) splited into: {str(divided_query)}")

    for i in range(len(divided_query)):
        if divided_query[i] not in [' + ', ' - ', ' * ', ' / ']:
            translated_query = TranslateFromOwnCommandToSQL(divided_query[i])
            sql_executed_with_translated_query = ExecuteSQL(cursor, translated_query)
            if sql_executed_with_translated_query != "N/D":
                divided_query[i] = str(sql_executed_with_translated_query)
    
    # print(divided_query)
    expression = "".join(divided_query)
    essa = eval(expression)

    Log(f"Query ({query}) executed successfully: {essa}")

    return essa

def TranslateFromOwnCommandToSQL(query):
    """
    Translates a SQL query by applying substitutions based on predefined translation patterns.

    Args:
        query (str): The original SQL query to be translated.

    Returns:
        str: The translated SQL query.
    """
    Log(f"Translating query: {query}")
    base_query = query
    for trans in translations:

        translation = re.sub(r'\(', r'\\(', trans)
        translation = re.sub(r'\)', r'\\)', translation)
        translation = re.sub(r'\$(\d+)', r'(\\S+)', translation)

        pattern = re.compile(translation)

        def replace_match(match):
            """
            Replaces matched groups in the query using the provided translation patterns.

            Args:
                match (re.Match): The match object containing the matched groups.

            Returns:
                str: The translated string with replaced patterns.
            """
            def rep2(match2):
                number = int(match2.group(1))
                return str(match.group(number))  

            new_string = re.sub(r'\$(\d+)', rep2, translations[trans])

            return new_string

        new_string = re.sub(pattern, replace_match, query)

        if query != new_string:
            args = GetArguments()
            new_string = re.sub(r'@([a-zA-Z_][a-zA-Z0-9_]*)', lambda match: args.get(match.group(1), f"@{match.group(1)}"), new_string)
            Log(f"Query ({base_query}) translated: {new_string}")
            return new_string
    
    args = GetArguments()
    query = re.sub(r'@([a-zA-Z_][a-zA-Z0-9_]*)', lambda match: args.get(match.group(1), f"@{match.group(1)}"), query)
    
    Log(f"Query ({base_query}) translated: {query}")

    return query

def ConnectToDatabase(password: str) -> Tuple[pyodbc.Cursor, pyodbc.Connection]:
    """
    Connects to the SQL Server database using the provided password.

    Args:
        password (str): The password for the SQL Server user.

    Returns:
        Tuple[pyodbc.Cursor, pyodbc.Connection]: The cursor and connection objects for interacting with the database.
    """
    Log("Connecting to SQL Server.")
    try:
        database_connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=Main;UID=sa;PWD=' + password, autocommit=True )
        Log("Connected to SQL Server successfully.")
        return database_connection.cursor(), database_connection
    except:
        Log("Connection to SQL Server failed.")
        exit()

def ExecuteSQL(cursor: pyodbc.Cursor, query: str) -> List:
    """
    Executes a given SQL query and returns the first result.

    Args:
        cursor (pyodbc.Cursor): The cursor object used to execute the query.
        query (str): The SQL query to execute.

    Returns:
        List: The first result from the query execution.
    """
    Log(f"Executing SQL: {query}.")
    try:
        cursor.execute(query)
        output = cursor.fetchall()[0][0]
        Log(f"SQL ({query}) execution successfull returning {str(output)}")
        if output is None:
            return 0
        return output
    except Exception as e:
        Log(f"!!! - SQL ({query}) execution failed returning {query}. Error: {e}")
        return "N/D"

def ReadTemplateFromPath(path: str) -> dict:
    """
    Reads a template file from the specified path and returns it as a dictionary.

    Args:
        path (str): The file path of the template to read.

    Returns:
        dict: A dictionary containing the ID, column names, and data from the template file.
    """
    Log(f"Reading template: {path}.")
    try:
        df = pd.read_csv(path, sep=";", names=["0","1","2"])
        template = {
            "ID": str(df[df["0"] == "ID"].values[0][1]),
            "Column Names": list(df.iloc[1].values),
            "Data": pd.DataFrame(df.iloc[2:].reset_index(drop=True))
        }
        Log(f"Reading template ({path}) successfull. Read ID: {template["ID"]}.")
        return template
    except:
        Log(f"!!! - Reading failed.")
        exit()


def SaveRaportToAFile(file: dict, path: str):
    """
    Saves the provided file dictionary as a CSV file at the specified path.

    Args:
        file (dict): The file dictionary containing 'ID', 'Column Names', and 'Data'.
        path (str): The file path where the report should be saved.
    """
    Log(f"Saving file with ID: {file["ID"]} to {path}.")
    file_to_save = [["ID", file["ID"], np.nan], file["Column Names"], *file["Data"].values.tolist()]
    file_to_save = pd.DataFrame(file_to_save)
    file_to_save.to_csv(path, sep=";", index=False, header=False)
    Log(f"File saved.")

def Log(message: str) -> None:
    """
    Logs a message with the current timestamp if logging is enabled.

    Args:
        message (str): The message to log.
    """
    from datetime import datetime
    if logging: # type: ignore
        formatted_datetime =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\033[90m[{formatted_datetime}] - {message}\033[0m")

def CloseDatabaseConnection(cursor: pyodbc.Cursor, database_connection: pyodbc.Connection) -> None:
    """
    Closes the provided database cursor and connection.

    Args:
        cursor (pyodbc.Cursor): The cursor object to close.
        database_connection (pyodbc.Connection): The database connection to close.
    """
    Log("Closing the connection to SQL Server.")
    cursor.close()
    database_connection.close()
    Log("Connection closed.")
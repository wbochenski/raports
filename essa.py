from typing import List, Tuple
import pandas as pd
import numpy as np
import pyodbc
import re

ENABLE_LOGGING = False
MEMORY = {}
RECURENCY_SAFEBLOCK = 5
TRANSLATIONS = {
    "SUM($1, $2)": "SELECT SUM($1) FROM Data WHERE ID LIKE '$2' AND Date > @date_from AND Date <= @date_to",
    "ESSA($1, $2)": "SELECT 69",
    "ESSA3($1)": "SELECT 69 - $1"
}

def Log(message: str) -> None:
    from datetime import datetime
    if ENABLE_LOGGING:
        formatted_datetime =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\033[90m[{formatted_datetime}] - {message}\033[0m")

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

def ConnectToDatabase(password: str) -> Tuple[pyodbc.Cursor, pyodbc.Connection]:
    Log("Connecting to SQL Server.")
    try:
        database_connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=Main;UID=sa;PWD=' + password, autocommit=True )
        Log("Connected to SQL Server successfully.")
        return database_connection.cursor(), database_connection
    except:
        Log("!!! - Connection to SQL Server failed.")
        exit()

def CloseDatabaseConnection(cursor: pyodbc.Cursor, database_connection: pyodbc.Connection) -> None:
    Log("Closing the connection to SQL Server.")
    cursor.close()
    database_connection.close()
    Log("Connection closed.")

def ReadTemplateFromPath(path: str) -> dict:
    Log(f"Reading template ({path}).")
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
        Log(f"!!! - Reading template ({path}) failed.")
        exit()

def SaveRaportToAFile(raport: dict, path: str) -> None:
    Log(f"Saving raport ({raport["ID"]}) to {path}.")
    file_to_save = [["ID", raport["ID"], np.nan], raport["Column Names"], *raport["Data"].values.tolist()]
    file_to_save = pd.DataFrame(file_to_save)
    file_to_save.to_csv(path, sep=";", index=False, header=False)
    Log(f"Raport ({raport["ID"]}) saved.")

def ParseTemplateToRaport(cursor: pyodbc.Cursor, template: dict) -> dict:
    Log(f"Parsing template ({template["ID"]}).")
    rows = template["Data"].values.tolist()
    for i in range(len(rows)):
        rows[i][2] = ExecuteAnything(cursor, rows[i][2], rows[i][0], template["ID"])
    
    for i in range(len(rows)):
        rows[i][2] = PartSum(cursor, rows[i][2], rows[i][0], template["ID"])
    
    calculated_raport = {
        "ID": template["ID"],
        "Column Names": template["Column Names"],
        "Data": pd.DataFrame(rows)
    }

    Log(f"Template ({template["ID"]}) parsed.")

    return calculated_raport

# ----------------------------------------------------------------------------------------------

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
    for trans in TRANSLATIONS:

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

            new_string = re.sub(r'\$(\d+)', rep2, TRANSLATIONS[trans])

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

def ExecuteSQL(cursor: pyodbc.Cursor, query: str) -> List:
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

def ExecuteAnything(cursor: pyodbc.Cursor, query: str, rowID: str, fileID: str) -> List:
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
            match1 = re.match(r"VAL\((.+)\)", divided_query[i])
            if match1:
                value = match1.group(1)
                if "," in value:
                    Log("essa")
                    value = value.split(",")
                    # raportID = value[0]
                    # rowID = value[1]
                    global RECURENCY_SAFEBLOCK    
                    if RECURENCY_SAFEBLOCK >= 0:
                        RECURENCY_SAFEBLOCK -= 1
                        template = ReadTemplateFromPath(f"./templates/{value[0]}.csv")
                        file = ParseTemplateToRaport(cursor, template)
                        divided_query[i] = str(MEMORY[file["ID"] + " " + str(value[1])])
                    else:
                        divided_query[i] = str(-9999999999)
                else:
                    divided_query[i] = str(MEMORY[fileID + " " + str(value)])
                continue
            translated_query = TranslateFromOwnCommandToSQL(divided_query[i])
            sql_executed_with_translated_query = ExecuteSQL(cursor, translated_query)
            if sql_executed_with_translated_query != "N/D":
                divided_query[i] = str(sql_executed_with_translated_query)
    
    # print(divided_query)
    expression = "".join(divided_query)
    if expression != "PartSum()":
        essa = eval(expression)
    else:
        essa = "PartSum()"

    MEMORY[fileID + " " + str(rowID)] = essa

    Log(f"Query ({query}) executed successfully: {essa}")

    return essa

def PartSum(cursor: pyodbc.Cursor, query: str, rowID: str, fileID: str) -> List:
    if query == "PartSum()":
        sum = 0

        for i in MEMORY:
            if i.startswith(fileID + " " + rowID + '.') and MEMORY[i] != "PartSum()":
                sum += MEMORY[i]

        return sum
    else:
        return query

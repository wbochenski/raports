from typing import List, Tuple
import pandas as pd
import numpy as np
import pyodbc
import re

FUNCTIONS = {
    "SUM($1, $2)": "SELECT SUM($1) FROM Data WHERE ID LIKE '$2' AND Date > @date_from AND Date <= @date_to",
    "ESSA($1, $2)": "SELECT 69",
    "ESSA3($1)": "SELECT 69 - $1"
}

ENABLE_LOGGING = True
MEMORY = {}
RECURENCY_SAFEBLOCK = 5

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
        rows[i][2] = ExecuteQuery(cursor, rows[i][2], rows[i][0], template["ID"])
    
    for i in range(len(rows)):
        rows[i][2] = ExecutePartSums(cursor, rows[i][2], rows[i][0], template["ID"])
    
    calculated_raport = {
        "ID": template["ID"],
        "Column Names": template["Column Names"],
        "Data": pd.DataFrame(rows)
    }

    Log(f"Template ({template["ID"]}) parsed.")

    return calculated_raport

def ExecuteSQL(cursor: pyodbc.Cursor, query: str) -> List:
    Log(f"Executing SQL ({query}) on Database.")
    try:
        cursor.execute(query)
        output = cursor.fetchall()[0][0]
        Log(f"SQL ({query}) execution successfull ({str(output)})")
        if output is None:
            return 0
        return output
    except Exception as e:
        Log(f"!!! - SQL ({query}) execution failed ({e})")
        return "N/D"

def ExecuteQuery(cursor: pyodbc.Cursor, query: str, rowID: str, templateID: str) -> List:
    
    def SubstitutePartsOfQuery(query: str) -> str:
        Log(f"Substitute query ({query})")

        def SubstituteFunction(query: str) -> str:
            for function in FUNCTIONS:

                regularExpression = re.sub(r'\(', r'\\(', function)
                regularExpression = re.sub(r'\)', r'\\)', regularExpression)
                regularExpression = re.sub(r'\$(\d+)', r'(\\S+)', regularExpression)

                pattern = re.compile(regularExpression)

                def ReplaceFunction(match):
                    def ReplaceParameters(match2):
                        parameter_value = int(match2.group(1))
                        return str(match.group(parameter_value))  
                    return re.sub(r'\$(\d+)', ReplaceParameters, FUNCTIONS[function])

                new_query = re.sub(pattern, ReplaceFunction, query)

                if query != new_query:
                    return new_query
            return query
        
        def SubstituteArguments(query: str) -> str:
            args = GetArguments()
            return re.sub(r'@([a-zA-Z_][a-zA-Z0-9_]*)', lambda match: args.get(match.group(1), f"@{match.group(1)}"), query)
        
        new_query = SubstituteArguments(SubstituteFunction(query))

        Log(f"Query ({query}) substitiuted ({new_query})")
        return new_query
    
    def CalculateDiffrentCell(query: str) -> str:
        match1 = re.match(r"VAL\((.+)\)", query)
        if match1:
            value = match1.group(1)
            if "," in value:
                value = value.split(",")
                global RECURENCY_SAFEBLOCK    
                if RECURENCY_SAFEBLOCK >= 0:
                    RECURENCY_SAFEBLOCK -= 1
                    template = ReadTemplateFromPath(f"./templates/{value[0]}.csv")
                    file = ParseTemplateToRaport(cursor, template)
                    return str(MEMORY[file["ID"] + " " + str(value[1])])
                else:
                    return str(-9999999999)
            else:
                return str(MEMORY[templateID + " " + str(value)])
        else:
            return query

    Log(f"Executing query ({query})")

    if pd.isna(query):
        return query
    
    subqueries = re.split(r'(\s[\+\-\*/]\s)', query)

    Log(f"Query ({query}) splited into: {str(subqueries)}")

    for i in range(len(subqueries)):
        if subqueries[i] not in [' + ', ' - ', ' * ', ' / ']:
            subqueries[i] = CalculateDiffrentCell(subqueries[i])
            translated_query = SubstitutePartsOfQuery(subqueries[i])
            sql_executed_with_translated_query = ExecuteSQL(cursor, translated_query)
            if sql_executed_with_translated_query != "N/D":
                subqueries[i] = str(sql_executed_with_translated_query)
    
    expression = "".join(subqueries)
    if expression != "PartSum()":
        result = eval(expression)
    else:
        result = "PartSum()"

    MEMORY[templateID + " " + str(rowID)] = result

    Log(f"Query ({query}) executed successfully: {result}")

    return result

def ExecutePartSums(cursor: pyodbc.Cursor, query: str, rowID: str, fileID: str) -> List:
    if query == "PartSum()":
        sum = 0
        for i in MEMORY:
            if i.startswith(fileID + " " + rowID + '.') and MEMORY[i] != "PartSum()":
                sum += MEMORY[i]
        return sum
    else:
        return query

db, connection = ConnectToDatabase('YourStrong.Passw0rd')

args = GetArguments()
Log(f"Arguments: {str(args)}")

template = ReadTemplateFromPath(f"./templates/{args["template"]}.csv")
raport = ParseTemplateToRaport(db, template)
SaveRaportToAFile(raport, f"./raports/{args["raport"]}.csv")

CloseDatabaseConnection(db, connection)
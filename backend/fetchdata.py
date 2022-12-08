import os
import pyodbc
import pandas as pd
import configparser
from datetime import datetime

def connect_database(var):
    config = configparser.ConfigParser()
    config.read('config_file.ini')

    db_server = config.get(var, "SERVER")
    db_database = config.get(var, "DATABASE")
    db_user = config.get(var, "UID")
    db_password = config.get(var, "PWD")

    conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',server=db_server, database=db_database,user=db_user, password=db_password)

    return conn

db = connect_database("prod")

def journey_data():

	cur = db.cursor()
	query = "select * from [dbo].[policy_table];"
	cur.execute(query)

	columns = [d[0] for d in cur.description]
	out = [dict(zip(columns, row)) for row in cur.fetchall()]

	cur.close()

	df = pd.DataFrame(out)
	df.to_excel("journey_report.xlsx")
	return df
    
if __name__ == '__main__':
	df = journey_data()
    
	print(df.head(20))

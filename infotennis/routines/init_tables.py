import argparse
import os
import warnings
warnings.filterwarnings("ignore")

from dotenv import load_dotenv
import pandas as pd
import pymysql
import yaml

from infotennis.routines.sql_functions import initalise_tables

# Load config file into dict 'configs'
with open("./config.yaml", "r") as yamlfile:
    configs = yaml.safe_load(yamlfile)

# Configure settings
load_dotenv()
host = os.getenv('MYSQL_HOST')
port = os.getenv('MYSQL_PORT')
user = os.getenv('MYSQL_USER')
password = os.getenv('DATABASE_PASSWORD')
database_name = os.getenv('DATABASE_NAME')

# Create a connection to the given database
conn = pymysql.connect(
    host=host,
    port=int(3306),
    user="root",
    passwd=password,
    db=database_name,
    charset='utf8mb4')

# This is the object used to interact with the database
# Do not create an instance of a Cursor yourself. Call connections.Connection.cursor().
mycursor = conn.cursor()

if __name__ == "__main__":
    try:
        initalise_tables(mycursor, database_name, table="all")
    except:
        import traceback, pdb, sys
        traceback.print_exc()
        print ('')
        pdb.post_mortem()
        sys.exit(1)


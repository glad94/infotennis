"""
The entire web-scraping->database updating pipeline is managed from this script!
"""
import datetime
import logging
import os
import time
import warnings
warnings.filterwarnings("ignore")

from dotenv import load_dotenv
from func_timeout import func_timeout, FunctionTimedOut
import pandas as pd
import pymysql
import yaml

from infotennis.scrapers.scrape_match_data import scrape_ATP_results_data
from infotennis.routines.sql_functions import insert_results_data_new, update_stat_tables_from_files
from infotennis.routines.update_calendar_results import get_tourns_toscrape, get_results_toscrape

# Load config file into dict 'configs'
with open("./config.yaml", "r") as yamlfile:
    configs = yaml.safe_load(yamlfile)

data_dir = configs["output"]['dir']
data_path = configs["output"]['path']
log_dir = configs["log"]['dir']

# Configure settings
load_dotenv()
host = os.getenv('MYSQL_HOST')
port = os.getenv('MYSQL_PORT')
user = os.getenv('MYSQL_USER')
password = os.getenv('DATABASE_PASSWORD')
database_name = os.getenv('DATABASE_NAME')

table_cal = "atp_calendars"
table_results = "atp_results"
table_stats = {"key-stats": "atp_key_stats",
            "rally-analysis": "atp_rally_analysis",
            "stroke-analysis": "atp_stroke_analysis",
            "court-vision": "atp_court_vision"}

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

# Log File Settings (create a new log file per month)
log_file = log_dir+f"infotennis_log_{datetime.datetime.now().year}{datetime.datetime.now().month}.log"
logging.basicConfig(filename=log_file,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO,
                    force=True)
# Suppress "WDM INFO ====== WebDriver manager ======" messages
os.environ['WDM_LOG_LEVEL'] = '0'


def run_update_routines(conn, database_name, data_dir, data_path, data_type="all", insert=True):
    """
    Run the ATP infotennis update routine, which includes multiple steps for updating the given database.

    Args:
        conn (pymysql.connections.Connection): The MySQL database connection.
        database_name (str): The name of the database where tables will be updated.
        data_dir (str): The directory where raw match statistics data is stored.
        data_path (str): The path to the data files, including placeholders for data type and year.
        data_type (str, optional): The type of data to update (e.g., "all", "key-stats", "rally-analysis", "stroke-analysis", "court-vision"). Defaults to "all".
        insert (bool, optional): Whether to insert data into the database. Defaults to True.

    This function runs the ATP infotennis update routine, which includes multiple steps for updating the database:

    - Step 1: Get and update the calendar table with the latest tournament information.
    - Step 2: Get and update the results table with the latest match results.
    - Step 3: Get and save raw match statistics data files.
    - Step 4: Process and update match statistics tables based on the data type.

    The routine logs the progress and execution time for each step and completes the update process for the database.
    """
    # Print and log that the routine has been started
    time_utc = str(pd.Timestamp.utcnow())
    print(f"ATP infotennis update routine has started at {time_utc} (UTC).")
    logging.info(f"===================================================================")
    logging.info(f"ATP infotennis update routine has started at {time_utc} (UTC).")

    ### Step 1
    print(f"Running Routine Step 1: Get and update calendar table.")
    st = time.time()
    df_tourns_updt = get_tourns_toscrape(table_cal, conn)

    # Update the respective DB table with the updated calendar
    print(f"Inserting new calendar data.")
    insert_results_data_new(mycursor, conn, database_name, table_cal, df_tourns_updt)
    et = time.time()
    elapsed_time = et - st
    print(f"Completed Routine Step 1 in {elapsed_time} seconds.")
    
    ### Step 2
    print(f"Running Routine Step 2: Get and update results table.")
    st = time.time()
    try:
        #breakpoint()
        df_results_update = func_timeout(300, get_results_toscrape, args=(table_results, df_tourns_updt, conn))
    except FunctionTimedOut:
        print ("Step 2 query for df_results_update could not complete within 5 min")
        return
    #df_results_update = get_results_toscrape(table_results, df_tourns_updt, conn)
    if len(df_results_update) == 0:
        print("No new ATP match results to add.")
        print("ATP infotennis update routine completed with no updates.")
        logging.info(f"ATP infotennis update routine completed at {str(pd.Timestamp.utcnow())} (UTC).")
        return
    else:
        print(f"Updating the Results Table with {len(df_results_update)} new results.")
    # Update the respective DB table with the updated calendar
    #breakpoint()
    try:
        func_timeout(30, insert_results_data_new, args=(mycursor, conn, database_name, table_results, df_results_update))
    except FunctionTimedOut:
        print ("Step 2 query for insert_results_data_new could not complete within 30 seconds")
        return
    #insert_results_data_new(mycursor, conn, database_name, table_results, df_results_update) 
    logging.info(f'Inserted {len(df_results_update)} new results to atp_results.')
    et = time.time()
    elapsed_time = et - st
    print(f"Completed Routine Step 2 in {elapsed_time} seconds.")

    ### Step 3
    print(f"Running Routine Step 3: Get and save raw match statistics data.")
    st = time.time()
    if data_type == "all":
        data_types = ["key-stats", "rally-analysis", "stroke-analysis", "court-vision"]
    else:
        if data_type not in ["key-stats", "rally-analysis", "stroke-analysis", "court-vision"]:
            print(f"Unrecognised data_type {data_type} provided!")
            print("ATP infotennis update routine completed after Step 2.")
            logging.info(f"ATP infotennis update routine completed at {str(pd.Timestamp.utcnow())} (UTC).")
            return
        else:
            data_types = [data_type]

    files_scraped = {}
    for d_type in data_types:
        files_scraped[f"{d_type}"] = scrape_ATP_results_data(data_dir, data_path, df_results_update, data_type=d_type,\
                                                            create_output_path=True)
    et = time.time()
    elapsed_time = et - st
    print(f"Completed Routine Step 3 in {elapsed_time} seconds.")

    ### Step 4 
    print(f"Running Routine Step 4: Process and update match statistics tables.")
    st = time.time()
    #breakpoint()
    for d_type in data_types:
        if files_scraped[f"{d_type}"]:
            table_stat = table_stats[f"{d_type}"]
            update_stat_tables_from_files(df_results_update, d_type, database_name, table_stat, mycursor, conn, data_dir, data_path, insert)

    et = time.time()
    elapsed_time = et - st
    print(f"Completed Routine Step 4 in {elapsed_time} seconds.")
    print(f"ATP infotennis update routine has completed at {str(pd.Timestamp.utcnow())} (UTC). Please view logfile for summary.") 
    logging.info(f"ATP infotennis update routine has completed at {str(pd.Timestamp.utcnow())} (UTC).")
    logging.info(f"===================================================================")   

if __name__ == "__main__":
    try:
        run_update_routines(conn, database_name, data_dir, data_path, data_type="all", insert=True)
    except:
        import traceback, pdb, sys
        traceback.print_exc()
        print ('')
        pdb.post_mortem()
        sys.exit(1)
    
"""
Created by Gerald on 23/9/2023 16:51 LT.

Functions for creating/deleting/updating the Database (MySQL).
"""
import datetime
import glob
import json
import logging
import os

import numpy as np
import pandas as pd
import yaml

from infotennis.processing.processing_keystats import process_key_stats
from infotennis.processing.processing_rallys import process_rally_analysis
from infotennis.processing.processing_strokes import process_stroke_analysis
from infotennis.processing.processing_courtvision import process_court_vision


# Suppress "WDM INFO ====== WebDriver manager ======" messages
os.environ['WDM_LOG_LEVEL'] = '0'


table_dtypes_all = {   
    "atp_calendars": "year INT, tournament VARCHAR(255), tournament_id VARCHAR(32), category VARCHAR(64), location VARCHAR(255),\
                date_start VARCHAR(32), tournament_status VARCHAR(32), draw VARCHAR(32), surface VARCHAR(64), finance VARCHAR(32),\
                winner VARCHAR(255), url VARCHAR(255)",
    "atp_results": "year INT, tournament VARCHAR(255), tournament_id VARCHAR(255), category VARCHAR(255), match_id VARCHAR(255),\
                round VARCHAR(255), player1_name VARCHAR(255), player1_id VARCHAR(255), player1_seed VARCHAR(255),\
                player1_nation VARCHAR(255), player2_name VARCHAR(255), player2_id VARCHAR(255), player2_seed VARCHAR(255),\
                player2_nation VARCHAR(255), score VARCHAR(255), url VARCHAR(255), court_vision INT",
    "key_stats": "year INT, tournament_id VARCHAR(32), match_id VARCHAR(32), round VARCHAR(32), sets_completed INT, set_n INT,\
                player_id VARCHAR(32), opponent_id VARCHAR(32), serve_rating INT, aces INT, serves_unreturned INT, double_faults INT,\
                serve1 VARCHAR(32), serve1_pct FLOAT, serve1_pts_won VARCHAR(32), serve1_pts_won_pct FLOAT, serve2_pts_won VARCHAR(32),\
                serve2_pts_won_pct FLOAT, break_points_saved VARCHAR(32), break_points_saved_pct FLOAT, service_games_played INT,\
                return_rating INT, serve1_return_pts_won VARCHAR(32), serve1_return_pts_won_pct FLOAT, serve2_return_pts_won VARCHAR(32),\
                serve2_return_pts_won_pct FLOAT, break_points_converted VARCHAR(32), break_points_converted_pct FLOAT, break_points_faced INT,\
                return_games_played INT, net_points_won VARCHAR(32), net_points_won_pct FLOAT, winners INT, unforced_errors INT,\
                service_points_won VARCHAR(32), service_points_won_pct FLOAT, return_points_won VARCHAR(32), return_points_won_pct FLOAT,\
                total_points_won VARCHAR(32), total_points_won_pct FLOAT, max_speed INT, serve1_avg_speed INT, serve2_avg_speed INT",
    "rally_analysis": "year INT, tournament_id VARCHAR(32), match_id VARCHAR(32), round VARCHAR(32), sets_completed INT,\
                shot_number VARCHAR(32), outcome VARCHAR(32), player_id VARCHAR(32), opponent_id VARCHAR(32), crucial_point TINYINT,\
                score VARCHAR(32), hand VARCHAR(32), point_end_type VARCHAR(32), point_id VARCHAR(32), serve INT, serve_dir VARCHAR(32),\
                court_side VARCHAR(32), serve_speed INT, set_n INT, game VARCHAR(32), point VARCHAR(32), shot_type VARCHAR(32),\
                p1_break_point TINYINT, p2_break_point TINYINT, p1_net_point TINYINT, p2_net_point TINYINT, tie_break TINYINT,\
                set_point TINYINT",
    "stroke_analysis": "year INT, tournament_id VARCHAR(32), match_id VARCHAR(32), round VARCHAR(32), sets_completed INT, set_n INT,\
                player_id VARCHAR(32), opponent_id VARCHAR(32), hand VARCHAR(32), shot_type VARCHAR(32), winners INT, errors INT,\
                unforced_errors INT, others INT",
    "court_vision": "year INT, tournament_id VARCHAR(32), match_id VARCHAR(32), round VARCHAR(32), p1_id VARCHAR(32), p2_id VARCHAR(32),\
                point_id VARCHAR(32), server_id VARCHAR(32), scorer_id VARCHAR(32), receiver_id VARCHAR(32), ball_speed_kmh FLOAT,\
                rally_length INT, point_end_type VARCHAR(32), stroke_type VARCHAR(32), serve_type VARCHAR(32), court VARCHAR(32),\
                set_n INT, game INT, point INT, serve INT, hand VARCHAR(32), break_point TINYINT, break_point_converted TINYINT,\
                p1_sets_w INT, p2_sets_w INT, p1_set_score INT, p2_set_score INT, p1_game_score VARCHAR(32), p2_game_score VARCHAR(32),\
                is_tiebreak INT, stroke_idx INT, x_hit FLOAT, y_hit FLOAT, z_hit FLOAT, x_peak_pre FLOAT, y_peak_pre FLOAT,\
                z_peak_pre FLOAT, x_net FLOAT, y_net FLOAT, z_net FLOAT, x_bounce FLOAT, y_bounce FLOAT, z_bounce FLOAT, x_peak_post FLOAT,\
                y_peak_post FLOAT, z_peak_post FLOAT"
}

def initalise_tables(mycursor, database_name, table="all"):
    """
    Initialize MySQL tables for storing tennis data.

    Args:
        mycursor (pymysql.cursors.Cursor): The MySQL cursor for executing queries.
        database_name (str): The name of the database where tables will be created.
        table (str, optional): The specific table to initialize. Defaults to "all".

    This function initializes MySQL tables to store tennis-related data. You can choose to initialize all the tables or
    a specific table by providing its name. The function creates tables with appropriate data types and, for some tables,
    unique indexes based on specific match identifying fields.

    Available table names:
    - atp_results
    - atp_calendars
    - atp_key_stats
    - atp_rally_analysis
    - atp_stroke_analysis
    - atp_court_vision
    """
    if table == "all":
        tables = ["atp_results", "atp_calendars", "atp_key_stats", "atp_rally_analysis", "atp_stroke_analysis", "atp_court_vision"]
    else:
        tables = [table]

    for table in tables:
        if table in ["atp_results", "atp_calendars"]:
            table_dtypes = table_dtypes_all[table]
        else:
            table_dtypes = table_dtypes_all["_".join(table.split("_")[1:])]

        mycursor.execute(f"CREATE TABLE {table} (id INT AUTO_INCREMENT PRIMARY KEY, "+\
                table_dtypes + ")")
        
        # Create a unique index based on the unique match identifying fields for following table types
        if table == "atp_calendars":
            mycursor.execute(f"CREATE UNIQUE INDEX year_tourn_id ON {database_name}.{table} (year, tournament_id);") 
        elif table == "atp_key_stats":
            mycursor.execute(f"CREATE UNIQUE INDEX unique_stat_row ON {database_name}.{table} (year, tournament_id, match_id, set_n, player_id);")
        elif table == "atp_stroke_analysis":
            mycursor.execute(f"CREATE UNIQUE INDEX unique_stat_row ON {database_name}.{table} (year, tournament_id, match_id, set_n, player_id, hand, shot_type);")
        elif table == "atp_rally_analysis":
            mycursor.execute(f"CREATE UNIQUE INDEX unique_stat_row ON {database_name}.{table} (year, tournament_id, match_id, point_id);")
        elif table == "atp_court_vision":
            mycursor.execute(f"CREATE UNIQUE INDEX unique_stat_row ON {database_name}.{table} (year, tournament_id, match_id, point_id, stroke_idx);")


def drops_tables(mycursor, database_name, table="all"):
    """
    Drops MySQL tables containing tennis data.

    Args:
        mycursor (pymysql.cursors.Cursor): The MySQL cursor for executing queries.
        database_name (str): The name of the database where tables will be dropped.
        table (str, optional): The specific table to drop. Defaults to "all".

    This function drops MySQL tables that store tennis-related data. You can choose to drop all the tables or
    a specific table by providing its name.

    Available table names:
    - atp_results
    - atp_calendars
    - atp_key_stats
    - atp_rally_analysis
    - atp_stroke_analysis
    - atp_court_vision
    """
    if table == "all":
        tables = ["atp_results", "atp_calendars", "atp_key_stats", "atp_rally_analysis", "atp_stroke_analysis" "atp_court_vision"]
    else:
        tables = [table]
    for table in tables:
        mycursor.execute(f"DROP TABLE " + database_name+"."+table)


def insert_results_data_new(mycursor, conn, database_name, table, dataframe, batch=False):
    """
    Inserts data from a DataFrame into a MySQL table, updating existing rows if a duplicate key is found.

    Args:
        mycursor (pymysql.cursors.Cursor): The MySQL cursor for executing queries.
        conn (pymysql.connections.Connection): The MySQL database connection.
        database_name (str): The name of the database where the table resides.
        table (str): The name of the table where data should be inserted.
        dataframe (pandas.DataFrame): The DataFrame containing the data to be inserted.
        batch (bool, optional): Flag indicating whether to insert data in batches. Defaults to False.

    This function inserts data from a DataFrame into a MySQL table. If a duplicate key is found, it updates the existing row
    instead of inserting a new one. The function dynamically generates the INSERT...ON DUPLICATE KEY UPDATE statement based
    on the column names in the table.

    The function supports batch insertion, which can be faster for large DataFrames. If batch mode is enabled, data is inserted
    in smaller batches (default batch size is 10) to improve performance. If batch mode is disabled, data is inserted row by row.

    After inserting or updating the data, the function sends a COMMIT statement to the MySQL server to commit the changes.
    """
    # Reset the id-column to auto-increment starting from +1 of the last entry's id 
    # Step 1: Find the current maximum "id" value
    mycursor.execute(f"SELECT MAX(id) FROM "+ database_name+"."+table)
    max_id = mycursor.fetchone()[0]
    
    # Step 2: Alter the table to set the auto-increment value
    if max_id is not None:
        mycursor.execute(f"ALTER TABLE "+ database_name+"."+table +f" AUTO_INCREMENT = {max_id + 1}")
    
    # Get all column names from the input DB table
    mycursor.execute("SHOW COLUMNS FROM "+database_name+"."+table)
    columns = [column[0] for column in mycursor.fetchall()][1:]
    column_value_pairs = [f"{column} = IF(VALUES({column}) IS NULL, {column}, VALUES({column}))" for column in columns]
    
    update_statement = "INSERT INTO " + table + " (" + ', '.join(columns) + ") " + \
        "VALUES" + "(" + ', '.join(['%s'] * len(columns)) + ")"\
        " ON DUPLICATE KEY UPDATE " + ", ".join(column_value_pairs)
        
    # Execute the insertion
    # Execute row-by-row or as a batch (default size of 20)
    if batch: 
        for bt in range(0, len(dataframe), 10):
            if bt+10 >  len(dataframe):
                mycursor.executemany(update_statement, [list(row) for i,row in dataframe.iloc[bt:].iterrows()])
            else:
                mycursor.executemany(update_statement, [list(row) for i,row in dataframe.iloc[bt:bt+10].iterrows()])
    else:
        # Loop thru the stats_processed DF and insert into the key_stats table
        for index, row in dataframe.iterrows():
            mycursor.execute(update_statement, list(row))

    # Sends a COMMIT statement to the MySQL server, committing the current transaction. Since by default Connector/Python does not 
    # autocommit, it is important to call this method after every transaction that modifies data for tables that use transactional 
    # storage engines
    conn.commit()

    return


def update_stat_tables_from_files(df_results_update, data_type, database_name, table, mycursor, conn, data_dir, data_path, insert=True):
    """_summary_

    Args:
        df_tourns_ref (_type_):      _description_
        df_tournres_updt (_type_):   Dataframe containing updated results with stats files to be added to the target table
        data_type (str):         {"key-stats", "rally-analysis", "stroke-analysis", "court-vision"}
        database_name:
        table (_type_):      _description_
        mycursor ():
        conn (_type_):              pymysql connection
        data_dir:
        data_path:
        insert:
    """
    
    n_stats_uploaded = 0 #Keep a count of how many match stats have been uploaded to the DB
    n_DNP = 0            #Keep a count of how many matches weren't actually played

    # Get the respective stat's DB table before the start of any processing/insertion
    df_stats_db = pd.read_sql_query(f"SELECT year,tournament_id,match_id FROM {database_name}.{table}",conn)

    for k, result in df_results_update.iterrows():
        tourn_id, round_n, year, match_id, player1_name, player2_name, score, court_vision = \
            result[["tournament_id", "round", "year", "match_id", "player1_name", "player2_name", "score", "court_vision"]]
        # Skip processing this result if it's not a court-vision match abd data_type isn't key-stats
        if court_vision != 1 and data_type != "key-stats":
            continue
        # Skip this result if it already exists in the DB stats table
        if match_id in df_stats_db[(df_stats_db.tournament_id == tourn_id) & (df_stats_db.year == year)].match_id.unique():
            continue
        if match_id is None:
            logging.info(f'No raw {data_type} file found for {year} {tourn_id}-{match_id}.')
            continue
        if player2_name == "Bye" or score == '(W())' or score == '(R())':
            n_DNP += 1
            continue

        # Locate the existing key-stats json file from the result's year, tournament_id and match_id
        file_stats = glob.glob(data_dir + data_path.replace("<data_type>", data_type).replace("<year>", str(year))  + f"{tourn_id}_*_{year}_{match_id.upper()}_{data_type}.json")
        # If no key-stats is found for the given match, note that match is missing stats file and continue
        if len(file_stats) == 0:
            logging.info(f'No raw {data_type} file found for {year} {tourn_id}-{match_id}.')
            continue
        else: # Else select the first list index
            file_stats = file_stats[0]
            # Open the rally-analysis file and read to a dict object
            with open(file_stats, 'r') as j:
                raw_data = json.loads(j.read())

        # Data processing function calls depending on the input data_type
        if data_type == "key-stats":
            # Extra Step to try and locate the corresponding rally-analysis file if data_type="key-stats"
            # Get the rally file path
            file_rallies = glob.glob(data_dir + data_path.replace("<data_type>","rally-analysis").replace("<year>", str(year)) + f"{tourn_id}_*_{year}_{match_id.upper()}_rally-analysis.json")
            # If no rally-analysis is found for the given match, set file_rallies to None
            if len(file_rallies) == 0:
                raw_rallys = None
            else: # Else select the first list index
                file_rallies = file_rallies[0]
                # Open the rally-analysis file and read to a dict object
                with open(file_rallies, 'r') as j:
                    raw_rallys = json.loads(j.read())
            df_stats_processed = process_key_stats(year, tourn_id, match_id, round_n, raw_data, raw_data_rallies=raw_rallys)

        elif data_type == "rally-analysis":
            df_stats_processed = process_rally_analysis(year, tourn_id, match_id, round_n, raw_data)
            # If non-unknown rows are fewer than 90% of the total points played, don't bother adding this to the DB
            if len(df_stats_processed[df_stats_processed.shot_number != "Unknown"]) < len(df_stats_processed)*0.9:
                continue
            else:
                df_stats_processed = df_stats_processed[df_stats_processed.shot_number != "Unknown"].reset_index(drop=True)

        elif data_type == "stroke-analysis":
            df_stats_processed = process_stroke_analysis(year, tourn_id, match_id, round_n, raw_data)
        
        elif data_type == "court-vision":
            df_stats_processed = process_court_vision(year, tourn_id, match_id, round_n, raw_data)
        else: 
            logging.error(f"Unrecognised data_type {data_type} provided.")
            return
        
        df_stats_processed = df_stats_processed.replace({np.nan: -999})
        if insert:
            if data_type != "key-stats":
                insert_results_data_new(mycursor, conn, database_name, table, df_stats_processed, batch=False)
            else:
                insert_results_data_new(mycursor, conn, database_name, table, df_stats_processed, batch=True)
        else: # Allow an insert=False option just for testing purposes, i.e. test whole pipeline but don't update the tables
            pass

        n_stats_uploaded += 1

    print(f'Inserted {data_type} for {n_stats_uploaded} matches out of {len(df_results_update)} (total), {len(df_results_update)-n_DNP} (played).')
    logging.info(f'Inserted {data_type} for {n_stats_uploaded} matches out of {len(df_results_update)} (total), {len(df_results_update)-n_DNP} (played).')
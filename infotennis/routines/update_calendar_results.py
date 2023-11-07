# Import required libraries
import datetime
import logging

import pandas as pd
from infotennis.scrapers.scraping_functions_atp import scrape_ATP_calendar, scrape_ATP_tournament

def get_tourns_toscrape(table, conn):
    """
    Compare the latest online version of the ATP calendar with the existing one in the database and return tournaments
    to scrape new match data for and update the database.

    Args:
        table (str): The name of the database table where the ATP calendar data is stored.
        conn (pymysql.connections.Connection): The MySQL database connection.

    Returns:
        pandas.DataFrame: A DataFrame containing tournaments that need new match data scraping and updating in the database.

    This function compares the latest online version of the ATP calendar with the existing one in the database and
    identifies tournaments that require new match data scraping and updating in the database. It retrieves the current
    year's ATP calendar, fetches the calendar data from the reference table in the database, and identifies differences
    between the two data sources.

    Tournaments that are marked as "Ongoing" are also considered for updates, ensuring that matches in progress are
    included in the list of tournaments to scrape.
    """
    ### 1. Retrieve the latest online ATP calendar + tournaments with match info (i.e. pending/started/completed)
    # Get the current year from system time
    year_now = datetime.datetime.now().year
    # Scrape the ATP calendar page at current time
    df_tourns_now = scrape_ATP_calendar(year_now)

    ### 2. Retrieve the latest ATP calendar page DataFrame from the reference table in our database
    df_tourns_db = pd.read_sql_query(f"SELECT * FROM {table} WHERE year = {year_now}",
    conn)

    # Keep only tournaments with valid information/results in the scraped dataframe
    df_tourns_wres = df_tourns_now[df_tourns_now.tournament_status!=""]

    # Get anti-join between 2 DFs to identify rows that are different btn the 2 DFs..
    outer_join = df_tourns_wres.merge(df_tourns_db.iloc[:,:], indicator=True, how='outer')
    anti_join = outer_join[~(outer_join._merge == 'both')]

    # "left_only" are those tournaments with updated information in their row compared with the existing table in the db
    df_tourns_updt = anti_join[anti_join["_merge"] == "left_only"]
    # Keep only columns that were in df_tourns
    df_tourns_updt = df_tourns_updt.loc[:, [col for col in df_tourns_now.columns]]
    # But I also want to keep tournaments that are ongoing...what if I scraped in the middle of the tourn?
    df_tourns_updt = pd.concat([df_tourns_updt, df_tourns_wres[df_tourns_wres.tournament_status=="Ongoing"]]).drop_duplicates()

    return df_tourns_updt


def get_results_toscrape(table, df_tourns_updt, conn):
    """
    Retrieve tournament results to scrape based on the tournaments with updated information.

    Args:
        table (str): The name of the database table where the ATP results data is stored.
        df_tourns_updt (pandas.DataFrame): DataFrame containing tournaments with updated information.
        conn (pymysql.connections.Connection): The MySQL database connection.

    Returns:
        pandas.DataFrame: A DataFrame containing tournament results to scrape and update in the database.

    This function retrieves tournament results that need to be scraped and updated in the database. It is based on the
    list of tournaments with updated information, and for each of these tournaments, it scrapes the latest match results
    and identifies differences between the new data and the existing database records.

    The function checks for the availability of new results and returns a DataFrame containing the results to be updated
    in the database.
    """
    # Get the current year from system time
    year_now = datetime.datetime.now().year

    ### 2. Retrieve the latest ATP results page DataFrame from the reference table in our database
    df_results_db = pd.read_sql_query(f"SELECT * FROM {table} WHERE year = {year_now}",
    conn)

    list_df_results_updt = []

    # Iterate through every tournament with updated results
    for i, row in df_tourns_updt.iterrows():
        df_results_newtourn = scrape_ATP_tournament(*row[["url", "tournament", "tournament_id", "year"]])
        # Check if the scraper returned a valid dataframe or not,
        # possible reason for failure, website outage, network issues etc.
        if df_results_newtourn is None:
            logging.info(f'Empty Dataframe returned for {row["url"]}.')
            continue
        df_results_newtourn.insert(3, "category", [row["category"]]*len(df_results_newtourn))
        df_results_newtourn.insert(4, "match_id", df_results_newtourn.url.apply(lambda x: x.split('/')[-1] if x != None else None))

        # Get anti-join between 2 DFs to identify rows that are different btn the 2 DFs..
        outer_join = df_results_newtourn.replace("",None).merge(df_results_db[df_results_db.tournament_id == row['tournament_id']].iloc[:,1:], indicator=True, how='outer')
        anti_join = outer_join[~(outer_join._merge == 'both')]

        # "left_only" are those tournaments with updated information in their row compared with the existing table in the db
        df_results_updt = anti_join[anti_join["_merge"] == "left_only"]
        # Keep only columns that were in df_tourns
        df_results_updt = df_results_updt.loc[:, [col for col in df_results_newtourn.columns]]

        if len(df_results_updt) == 0:
            logging.info(f'No new results found for {row["tournament"]}-{row["year"]}.')
            continue
        else:
            logging.info(f'{len(df_results_updt)} new results found for {row["tournament"]}-{row["year"]}.')
        
        list_df_results_updt.append(df_results_updt.iloc[::-1])
    
    if len(list_df_results_updt) == 0:
        df_results_update = pd.DataFrame()
    else:
        df_results_update = pd.concat(list_df_results_updt)
    return df_results_update
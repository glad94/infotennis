##########################################
# Created 29/3/2023 by Gerald Lim
# Web-Scraping Functions for the ATP Website (a.o. March 2023)
import logging
import os

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.edge.options import Options
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.microsoft  import EdgeChromiumDriverManager
import yaml


# Web-scraping utitilies
headers = {'User-Agent': 
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'} 

# Setting selenium options
options = Options()
options.headless = False
options.add_experimental_option('excludeSwitches', ['enable-logging'])

# Suppress "WDM INFO ====== WebDriver manager ======" messages
os.environ['WDM_LOG_LEVEL'] = '0'
logging.getLogger('WDM').setLevel(logging.NOTSET)

# Load config file into dict 'configs'
with open("./config.yaml", "r") as yamlfile:
    configs = yaml.safe_load(yamlfile)

##############################################
# Functions Start Here
def scrape_ATP_calendar(year):
    """Scrapes ATP Tournament Info for a given Calendar Year/Season.

    Args:
        year (int): Calendar year for which to scrape information from.

    Returns:
        pd.DataFrame: Contains data of the ATP calendar for the given year, with the following columns: 
            - year	
            - tournament	
            - category	
            - location	
            - date_start	
            - draw	
            - surface	
            - finance	
            - winner	
            - url
    """
    # ATP Tournament Archive Page URL
    url = configs['atp']['calendar'] % {'year': year}

    # Resets Driver
    service = EdgeService(executable_path=EdgeChromiumDriverManager().install())
    driver = webdriver.Edge(service=service, options=options)

    # Get URL
    driver.get(url)

    # Maxmimise the browser window
    driver.maximize_window()

    # Return lists of tournament info
    list_elem_info = driver.find_elements(By.CSS_SELECTOR, "#scoresResultsArchive > table > tbody > tr")

    # Initialise lists to store all the tournament info
    tourn_id_list = []
    category_list = []
    name_list = []
    location_list = []
    date_list = []
    tourn_stat_list = []
    draw_list = []
    surface_list = []
    finance_list = []
    winners_list = []
    url_list = []

    # Loop through list of elements and append info to individual lists
    for elem in list_elem_info:
        row_elems = elem.find_elements(By.TAG_NAME, "td")

        # Get the asset path for the tournament level/category stamp e.g. ATP 500
        try:
            tourn_stamp = elem.find_element(By.TAG_NAME, "img").get_attribute("src").split('/')[-1]
            if tourn_stamp == 'categorystamps_grandslam.png':
                category = "Grand Slam"
            elif tourn_stamp == 'categorystamps_1000.png':
                category = "ATP Masters 1000"
            elif tourn_stamp == 'categorystamps_500.png':
                category = "ATP 500"
            elif tourn_stamp == 'categorystamps_250.png':
                category = "ATP 250"
            else:
                category = "Other"
        except:
            category = "Other"
        # Tourn name, loc and start date
        name, location, date = row_elems[2].text.split('\n')
        # Draw Size for singles and doubles
        draw = row_elems[3].text
        # Surface
        surface = row_elems[4].text
        # Total Financial Commitment
        finance = row_elems[5].text
        # Winners for all categories
        winners = ", ".join(row_elems[6].text.split('\n'))

        # Presence of the 'RESULTS' text on this element indicates a result page is available
        if row_elems[7].text == 'RESULTS':
            url = row_elems[7].find_element(By.TAG_NAME, "a").get_attribute("href")
        else:
            url = ""
        # Get the tournament's status (e.g. ongoing, completed)
        # Check if the results page url is present
        if len(url) > 0:
            status = url.split('/')[-1]
            # if the url contains "results", it means the tournament has completed
            if status == "results" and winners != "":
                tourn_stat = "Completed"
            else:
                tourn_stat = "Ongoing"
        # No data yet
        else:
            tourn_stat = ""
        
        #Get the tournament ID from each tournament's overview page url
        try: 
            tourn_id = row_elems[2].find_element(By.TAG_NAME, "a").get_attribute("href").split("/")[-2]
        except:
            tourn_id = None

        # Replace the "live-scores" section of the url to "results" so for ongoing tournaments, the url will link
        # to the results page rather than the live scores one.
        url = url.replace("live-scores", "results")

        # Append list elements
        tourn_id_list.append(tourn_id)
        category_list.append(category)
        name_list.append(name)
        location_list.append(location)
        date_list.append(date)
        tourn_stat_list.append(tourn_stat)
        draw_list.append(draw)
        surface_list.append(surface)
        finance_list.append(finance)
        winners_list.append(winners)
        url_list.append(url)

    year_list = [year]*len(name_list)

    # Store lists into a DataFrame
    df_tourns = pd.DataFrame({"year": year_list, "tournament": name_list, "tournament_id":tourn_id_list, "category": category_list, \
                            "location": location_list, "date_start": date_list, "tournament_status": tourn_stat_list, "draw": draw_list,\
                            "surface": surface_list, "finance": finance_list, "winner": winners_list, "url": url_list})

    driver.quit()

    return df_tourns

def scrape_ATP_tournament(url, tournament, tournament_id, year):

    """Scrapes ATP Tournament Results/Info for a given Tournament.

    Args:
        url (str): URL of the chosen tournament's results page, retrievable from df_tourns['URL'].
        
        tournament (str): The selected tournament's name.

        tournament_id (str): The selected tournament's ID assigned on the ATP website.

        year (int): Calendar year for which to scrape information of the tournament from.

    Returns:
        pd.DataFrame : Contains data of the scraped tournament, with the following columns: 
            - round, 
            - player1_name, 
            - player1_id, 
            - player1_seed, 
            - player1_nation: flag_1_list, 
            - player2_name, 
            - player2_id, 
            - player2_seed
            - player2_nation
            - score
            - url
    """

    # Resets Driver
    service = EdgeService(executable_path=EdgeChromiumDriverManager().install())
    driver = webdriver.Edge(service=service, options=options)

    # Get URL
    driver.get(url)

    # Maxmimise the browser window
    driver.maximize_window()

    tournament_rounds = [ elem.text for elem in driver.find_elements(By.CSS_SELECTOR, ".day-table > thead") ]

    # Initialise list to contain for a single tournament, dataframes of match infos per round
    list_df_tourn = []

    # Find matches per round (i.e. final, semi-final, etc.)
    for i, round in enumerate(tournament_rounds):

        round_matches = driver.find_elements(By.CSS_SELECTOR, ".day-table > tbody")[i].find_elements(By.XPATH, "tr")

        seed_1_list = []
        seed_2_list = []
        flag_1_list = []
        flag_2_list = []
        player_1_list = []
        player_1_id_list = []
        player_2_list = []
        player_2_id_list = []
        score_list = []
        url_list = []
        cv_avail_list = []

        for match in round_matches:
            # Player Seedings
            seed_1,seed_2 = [ seed_elem.text.replace('(','').replace(')','') for seed_elem in match.find_elements(By.XPATH, "td[@class='day-table-seed']") ]
            # Player Flags (Nationalities)
            flag_elems = match.find_elements(By.XPATH, "td[@class='day-table-flag']")
            flags = []
            for flag_elem in flag_elems:
                try:
                    flag = flag_elem.find_element(By.TAG_NAME, "img").get_attribute("alt")
                    flags.append(flag)
                except NoSuchElementException:
                    flags.append("")
            flag_1, flag_2 = flags
            #flag_1, flag_2 = [flag_elem.find_element(By.TAG_NAME, "img").get_attribute("alt") for flag_elem in match.find_elements(By.XPATH, "td[@class='day-table-flag']") ]
            # Player Names
            player_elems = match.find_elements(By.XPATH, "td[@class='day-table-name']")
            player_1, player_2 = [player_elem.text for player_elem in player_elems ]
            player_1_id = player_elems[0].find_element(By.TAG_NAME, "a").get_attribute("href").split('/')[-2].upper()
            if player_2 != "Bye":
                player_2_id = player_elems[1].find_element(By.TAG_NAME, "a").get_attribute("href").split('/')[-2].upper()
            else:
                player_2_id = ""
            # Score
            score = match.find_element(By.XPATH, "td[@class='day-table-score']").text
            score = " ".join( [s[:2]+f"({s[-1]})" if len(s)>2 else s for s in score.split(' ')] )
            # Match Page URL
            try:
                url = match.find_element(By.XPATH, "td[@class='day-table-score']").find_element(By.TAG_NAME, "a").get_attribute("href")
                
            except NoSuchElementException:
                url = ""
                cv_avail = 0
            if url != "":
                try:
                    # Check if 2nd Screen / Court Vision is available for current match
                    # If 2nd screen is avail, it should be positioned as the 2nd day-table-button element (after "H2H")
                    cv_check = match.find_elements(By.XPATH, "td[@class='day-table-button']")[-1].find_element(By.TAG_NAME, "a").text
                    if cv_check == "2ND":
                        cv_avail = 1
                    else:
                        cv_avail = 0
                except NoSuchElementException:
                    cv_avail = 0
            seed_1_list.append(seed_1)
            seed_2_list.append(seed_2)
            flag_1_list.append(flag_1)
            flag_2_list.append(flag_2)
            player_1_list.append(player_1)
            player_1_id_list.append(player_1_id)
            player_2_list.append(player_2)
            player_2_id_list.append(player_2_id)
            score_list.append(score)
            url_list.append(url)
            cv_avail_list.append(cv_avail)

        round_list = [round]*len(score_list)

        # Create Dataframe for current tournament round 
        
        df_round_info = pd.DataFrame({"round": round_list, "player1_name": player_1_list, "player1_id": player_1_id_list, "player1_seed": seed_1_list, "player1_nation": flag_1_list, 
                                    "player2_name": player_2_list, "player2_id": player_2_id_list, "player2_seed": seed_2_list, "player2_nation": flag_2_list, "score": score_list,
                                    "url": url_list, "court_vision": cv_avail_list})


        # Append to list_df_tourn
        list_df_tourn.append(df_round_info)

    if len(list_df_tourn) == 0:
        return 
    # Concatenate the list of dataframes into one
    df_tourn_matches = pd.concat(list_df_tourn)
    df_tourn_matches = df_tourn_matches.reset_index(drop=True)
    # Add a column for Year
    df_tourn_matches.insert(0, "year", [year]*len(df_tourn_matches))
    # Add a column for tournament name
    df_tourn_matches.insert(1, "tournament", [tournament]*len(df_tourn_matches))
    # Add a column for tournament id
    df_tourn_matches.insert(2, "tournament_id", [str(int(tournament_id))]*len(df_tourn_matches))

    # Quit the browser
    driver.quit()

    return df_tourn_matches



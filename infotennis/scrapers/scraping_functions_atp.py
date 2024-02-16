##########################################
# Created 29/3/2023 by Gerald Lim
# Web-Scraping Functions for the ATP Website (a.o. Feb 2024)
import datetime
import json
import calendar
import logging
import os
import requests

from bs4 import BeautifulSoup
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
script_dir = os.path.dirname(__file__)
config_path = os.path.join(script_dir, "../../config.yaml")
#with open("./config.yaml", "r") as yamlfile:
with open(config_path, "r") as yamlfile:
    configs = yaml.safe_load(yamlfile)

##############################################
# Functions Start Here
month_dict = dict((v, k) for k, v in enumerate(calendar.month_abbr))

def parse_dates(dates_string):
    date_st, date_end = dates_string.split(" - ")
    # Parse the tournament end date first
    day_end, mon_end, yr_end = date_end.replace(", "," ").split(' ')
    mon_end = month_dict[mon_end[:3]] # Convert month name into numeric
    # Parse the tournament start date conditionally
    if len(date_st.replace(", "," ").split(' ')) == 3: # Start Date is provided as full DD Month YYYY
        day_st, mon_st, yr_st = date_st.replace(", "," ").split(' ')
        mon_st = month_dict[mon_st[:3]] # Convert month name into numeric
    elif len(date_st.replace(", "," ").split(' ')) == 2: # Start Date is provided as DD Month
        day_st, mon_st = date_st.split(' ')
        mon_st = month_dict[mon_st[:3]] # Convert month name into numeric
        yr_st = yr_end
    elif len(date_st.replace(", "," ").split(' ')) == 1: # Start Date is provided as full DD Month YYYY
        day_st = date_st[0]
        mon_st = mon_end
        yr_st = yr_end

    date_string_formatted = f"{yr_st}.{int(mon_st):02}.{int(day_st):02} - {yr_end}.{int(mon_end):02}.{int(day_end):02}"

    return date_string_formatted

def parse_winners(elems_winners):
    winner_list = []
    formats = []
    for elem in elems_winners:
        format = elem.find("dt").text
        
        if format == "Singles Winner":
            format = "SGL"
        elif format == "Doubles Winners":
            format = "DBL"
        elif format == "Team Winners":
            format = "Team"
        formats.append(format)

        winners = []
        for elem_winner in elem.find_all("dd"):
            if elem_winner.find("a") is None:
                winner = elem_winner.text.strip("\r\n                                    ")
            else:
                winner = elem_winner.find("a").text.strip("\r\n                                    ")
            winners.append(winner)
        winner_string = f"{format}: {' '.join(winners)}"
        winner_list.append(winner_string)

    return ", ".join(winner_list)    

def parse_player_scores(elem_player_match_score):
    player_scores = []
    for set_score in elem_player_match_score.find_all("div", class_="score-item"):
        score_list = set_score.find_all("span")
        if len(score_list) == 0:
            continue
        else:
            if len(score_list) == 1:
                score = score_list[0].text
            elif len(score_list) == 2:
                score = score_list[0].text
                score_tb = score_list[1].text
                score = f"{score}({score_tb})"
        player_scores.append(score)
    return player_scores

def move_bracketed_parts(lst):
    new_lst = []
    for item in lst:
        bracketed_part = ""
        remaining_part = item
        if '(' in item and ')' in item:
            bracket_start = item.index('(')
            bracket_end = item.index(')')
            bracketed_part = item[bracket_start:bracket_end+1]
            remaining_part = item[:bracket_start] + item[bracket_end+1:]
        new_item = remaining_part + bracketed_part
        new_lst.append(new_item)
    return new_lst

def parse_match_score(elems_match_score):
    
    player_1_score = parse_player_scores(elems_match_score[0])
    player_2_score = parse_player_scores(elems_match_score[1])

    score_list = [f"{i}{j}" for i,j in zip(player_1_score,player_2_score)]
    # Convert score into string format
    score_str = " ".join(move_bracketed_parts(score_list))

    return score_str   

def parse_match_content(elem_match):
    
    round_ = elem_match.find("strong").text.split(" - ")[0].replace("-","")
    round_ = " ".join([word.capitalize() for word in round_.split()])
    url_atp = "https://www.atptour.com"
    if elem_match.find("div", class_="match-cta") is None:
        url = ""
    else:
        if elem_match.find("div", class_="match-cta").find("a", string=lambda text: text and ("Match Stats" in text or "Stats" in text)) is not None:
            url = url_atp + elem_match.find("div", class_="match-cta").find("a", string=lambda text: text and ("Match Stats" in text or "Stats" in text))["href"]
        else:
            url = ""
    match_id = url.split("/")[-1]

    score = parse_match_score(elem_match.find_all("div", class_="scores"))

    elem_players = elem_match.find_all("div", class_="name")
    elem_pinfos = [e.find({"a", "p"}) for e in elem_players]
    if len(elem_players) == 2:
        #elem_pinfos = [e.find({"a", "p"}) for e in elem_players]
        player_1, player_2 = [" ".join(e.contents[0].text.split()) for e in elem_pinfos]
        player_1_seed, player_2_seed = [e.find("span").text.strip("()") for e in elem_players]
        player_1_id, player_2_id = [e["href"].split("/")[-2] if e.has_attr("href") else "" for e in elem_pinfos]
        player_1_flag, player_2_flag = [e['src'].split("/")[-1].split(".svg")[0] for e in elem_match.find_all("img", alt={"Player Flag"})]
    if len(elem_players) == 4:
        player_1a, player_1b, player_2a, player_2b = [" ".join(e.contents[0].text.split()) for e in elem_pinfos]
        player_1 = player_1a + ", " + player_1b
        player_2 = player_2a + ", " + player_2b
        player_1_seed, player_2_seed = [e.find("span").text.strip("()") for e in elem_players][::2]
        player_1a_id, player_1b_id, player_2a_id, player_2b_id = [e["href"].split("/")[-2] if e.has_attr("href") else "-" for e in elem_pinfos]
        player_1_id = player_1a_id + ", " + player_1b_id
        player_2_id = player_2a_id + ", " + player_2b_id

        player_1a_flag, player_1b_flag, player_2a_flag, player_2b_flag = [e['src'].split("/")[-1].split(".svg")[0] for e in elem_match.find_all("img", alt={"Player Flag", "Partner Flag"})]
        player_1_flag = player_1a_flag + ", " + player_1b_flag
        player_2_flag = player_2a_flag + ", " + player_2b_flag

    dict_match = {"round": round_, "player1_name": player_1, "player1_id": player_1_id, "player1_seed": player_1_seed, "player1_nation": player_1_flag, 
                                    "player2_name": player_2, "player2_id": player_2_id, "player2_seed": player_2_seed, "player2_nation": player_2_flag, "score": score,
                                    "url": url, "court_vision": 0}
    
    return dict_match 

def scrape_ATP_calendar(year: int):
    """
    Scrapes ATP Tournament Info for a given Calendar Year/Season.

    Args:
        year (int): Calendar year for which to scrape information from.

    Returns:
        df_tourns (pd.DataFrame): Contains data of the ATP calendar for the given year, with the following columns: 
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

    page = url
    #print (page)
    pageTree = requests.get(page, headers=headers)
    pageSoup = BeautifulSoup(pageTree.content, 'html.parser') 

    elems_events = pageSoup.find_all("ul", class_="events")

    tournament_list = [elm.find("span", class_="name").text for elm in elems_events]
    tourn_id_list = [elm.find("a", class_="tournament__profile")["href"].split('/')[-2] for elm in elems_events]
    location_list = [elm.find("span", class_="venue").text.strip(" | ") for elm in elems_events]
    date_list = [elm.find("span", class_="Date").text.strip(" | ") for elm in elems_events]
    date_list = [parse_dates(date) for date in date_list]

    elems_winners = [elm.find_all("dl", class_="winner") for elm in elems_events]
    winners_list = [parse_winners(el) for el in elems_winners]

    url_atp = "https://www.atptour.com"
    elems_urls = [elm.find("div", class_="non-live-cta").find("a") for elm in elems_events]
    url_list = [url_atp+elem["href"] if elem is not None else "" for elem in elems_urls]

    tourn_stat_list = []
    for url in url_list:
        if len(url.split("/")) > 1:
            if url.split("/")[5] == "archive":
                status = "Completed"
            elif url.split("/")[5] == "current":
                status = "Ongoing"
            else:
                status = ""
        else:
            status = ""
        tourn_stat_list.append(status)

    category_list= []
    for elm in elems_events:
        cat_png = elm.find("img", class_="events_banner")["src"].split("/")[-1]
        if cat_png == 'categorystamps_1000.png':
            category = 'ATP Masters 1000'
        elif cat_png == 'categorystamps_500.png':
            category = 'ATP 500'
        elif cat_png == 'categorystamps_250.png':
            category = 'ATP 250'
        elif cat_png == 'categorystamps_grandslam.png':
            category = "Grand Slam"
        else:
            category = "Other"
        category_list.append(category)

    year_list = [year]*len(tournament_list)
    surface_list = finance_list = draw_list = ["-"]*len(tournament_list)

    # Store lists into a DataFrame
    df_tourns = pd.DataFrame({"year": year_list, "tournament": tournament_list, "tournament_id":tourn_id_list, "category": category_list, \
                            "location": location_list, "date_start": date_list, "tournament_status": tourn_stat_list, "draw": draw_list,\
                            "surface": surface_list, "finance": finance_list, "winner": winners_list, "url": url_list})

    # Added 16/2/24
    # Surface, Finance and Draw info needs to be sourced from the "Tournaments" page (which will only have info for the current year)
    # as these are no longer present in the "Results Archive" page. Thankfully the tournaments data can be found in a simple JSON format
    
    # note: This will not work if the scraped year is not the current year because this page only exists for the current year
    if year == datetime.datetime.now().year:
        url_tournaments = "https://www.atptour.com/en/-/tournaments/calendar/tour"
        page = url_tournaments
        pageTree = requests.get(page, headers=headers)
        pageSoup = BeautifulSoup(pageTree.content, 'html.parser')

        results_json = json.loads(str(pageSoup))
        # The tournament data is segregated by months, so we need to concat them together
        df_tournaments_live = pd.concat([ pd.DataFrame(month_data['Tournaments'])  for month_data in results_json['TournamentDates'] ]).reset_index(drop=True)
        # Data formatting to conform to existing schema
        df_tournaments_live['surface'] = df_tournaments_live.IndoorOutdoor + " " + df_tournaments_live.Surface
        df_tournaments_live['draw'] = df_tournaments_live.apply(lambda x: f"SGL {x.SglDrawSize} DBL {x.DblDrawSize}", axis=1)
        df_tournaments_live['finance'] = df_tournaments_live.TotalFinancialCommitment
        # Merge this "live" tournament dataframe with the existing one to make sure the indexes match
        df_merged = df_tourns[["tournament_id"]].merge(df_tournaments_live[["Id", "surface", "draw", "finance"]], left_on='tournament_id', right_on='Id')
        assert len(df_merged) == len(df_tourns) 
        # Now assign the columns
        df_tourns['draw'] = df_merged['draw']
        df_tourns['surface'] = df_merged['surface']
        df_tourns['finance'] = df_merged['finance']

    return df_tourns

def scrape_ATP_tournament(url: str, tournament: str, tournament_id: str, year: int, format="S"):
    """
    Scrapes ATP Tournament Results/Info for a given tournament.
    
    Currently does not support team-based tournaments (e.g. United/ATP/Laver Cup) due to different 
    page layout used.

    Args:
        url (str): URL of the chosen tournament's results page, retrievable from df_tourns['URL'].
        tournament (str): The selected tournament's name.
        tournament_id (str): The selected tournament's ID assigned on the ATP website.
        year (int): Calendar year for which to scrape information of the tournament from.
        format (str, optional): Indicates tournament format to scrape results for, "S" (singles)
        or "D" (doubles). Also defaults back to "S" if an invalid arg is provided.

    Returns:
        df_tourn_matches (pd.DataFrame) : Contains results data of the scraped tournament, with the following columns: 
            - round, 
            - player1_name, 
            - player1_id, 
            - player1_seed, 
            - player1_nation,
            - player2_name, 
            - player2_id, 
            - player2_seed,
            - player2_nation,
            - score,
            - url
    """

    if format == "S":
        url = url + "?matchType=singles"
    elif format == "D":
        url = url + "?matchType=doubles"
    else:
        print("An invalid 'format' arg was provided! Defaulting to 'S'...")
        url = url + "?matchType=singles"

    page = url
    pageTree = requests.get(page, headers=headers)
    pageSoup = BeautifulSoup(pageTree.content, 'html.parser') 

    elem_days = pageSoup.find_all("div", class_="atp_accordion-item")
    elem_matches = [e.find_all("div", class_="match") for e in elem_days]

    list_df_matches = []
    for elem_round in elem_matches:
        for elem_match in elem_round:
            df_match = pd.DataFrame([parse_match_content(elem_match)])
            list_df_matches.append(df_match)

    df_tourn_matches = pd.concat(list_df_matches).reset_index(drop=True)

    # Add a column for Year
    df_tourn_matches.insert(0, "year", [year]*len(df_tourn_matches))
    # Add a column for tournament name
    df_tourn_matches.insert(1, "tournament", [tournament]*len(df_tourn_matches))
    # Add a column for tournament id
    df_tourn_matches.insert(2, "tournament_id", [str(int(tournament_id))]*len(df_tourn_matches))

    return df_tourn_matches
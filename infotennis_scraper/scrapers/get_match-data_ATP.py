"""
Scraping Functions/Methods for ATP Match Level Data
- Match Stats
- Rally Analysis
- Stroke Analysis
- Court Vision

v1 created by @glad94 15/8/2023
last tested on ATP sites on 15/8/2023

"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
import json
import itertools
import sys
sys._enablelegacywindowsfsencoding() #Deal with pandas problem with reading file with accents in file path i.e Alexis Sánchez, Victor Lindelöf 

import glob

headers = {'User-Agent': 
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'} 

from ast import literal_eval

import base64
import cryptography.hazmat.backends
import cryptography.hazmat.primitives.ciphers
import cryptography.hazmat.primitives.ciphers.algorithms
import cryptography.hazmat.primitives.ciphers.modes
import cryptography.hazmat.primitives.padding

import sys
from time import sleep
import datetime


##############################################
# Decrypting Utilities
def formatDate(t):
    """_summary_

    Args:
        t (_type_): _description_

    Returns:
        _type_: _description_
    """
    #e = datetime.datetime.now().utcoffset().total_seconds() / 60       # ChatGPT suggestion but not needed
    #t = t + datetime.timedelta(minutes=e)                              # ChatGPT suggestion but not needed
    
    t_tstamp = datetime.datetime.utcfromtimestamp(t/1000)
    n = t_tstamp.day
    r = int(str(n if n >= 10 else "0" + str(n))[::-1])
    i = t_tstamp.year
    a = int(str(i)[::-1])
    o = np.base_repr(int(str(t), base=16), 36).lower() + np.base_repr((i + a) * (n + r), 24).lower()
    s = len(o)
    if s < 14:
        o += "0" * (14 - s)
    elif s > 14:
        o = o[:14]
    return "#" + o + "$"

def decode(data):
    """_summary_

    Args:
        data (_type_): _description_

    Returns:
        _type_: _description_
    """
    e = formatDate(data['lastModified'])
    n = e.encode()
    r = e.upper().encode()
    cipher = cryptography.hazmat.primitives.ciphers.Cipher(
        cryptography.hazmat.primitives.ciphers.algorithms.AES(n),
        cryptography.hazmat.primitives.ciphers.modes.CBC(r),
        backend=cryptography.hazmat.backends.default_backend()
    )
    decryptor = cipher.decryptor()
    i = decryptor.update(base64.b64decode(data['response'])) + decryptor.finalize()
    unpadder = cryptography.hazmat.primitives.padding.PKCS7(128).unpadder()
    #return json.loads(unpadder.update(i) + unpadder.finalize().decode('utf-8'))
    return json.loads(i.decode("utf-8").replace(i.decode("utf-8")[-1],""))


##############################################
# Functions Start Here
def get_ATP_match_data(year, tourn_id, match_id, round_n, player1, player2, data_type):
    """
    Args:
        year (int): _description_
        tourn_id (str): _description_
        match_id (str): _description_
        round_n (str): _description_
        player1 (str): _description_
        player2 (str): _description_
        data_type (str): _description_

    Raises:
        ValueError: _description_

    Returns:
        _type_: _description_
    """
        # _,_,_,_,_,_,_,year,tourn_id,match_id = matches_scp.url.iloc[i].split('/')
    
    match_id = match_id.upper()
    # #print(match_id)

    match data_type:
        case "key-stats":
            link = f"https://itp-atp-sls.infosys-platforms.com/static/prod/stats-plus/{year}/{tourn_id}/{match_id}/keystats.json"
        case "rally-analysis":
            link = f"https://itp-atp-sls.infosys-platforms.com/static/prod/rally-analysis/{year}/{tourn_id}/{match_id}/data.json"
        case "stroke-analysis":
            link = f"https://itp-atp-sls.infosys-platforms.com/static/prod/stroke-analysis/v2/{year}/{tourn_id}/{match_id}/data.json"
        case "court-vision":
            link = f"https://itp-atp-sls.infosys-platforms.com/static/prod/court-vision/{year}/{tourn_id}/{match_id}/data.json"
        case _:
            raise ValueError("Invalid data_type argument provided.")
    
    # Get request and content from the given link and parse into HTML
    pageTree = requests.get(link, headers=headers)
    pageSoup = BeautifulSoup(pageTree.content, 'html.parser') 

    results_json = json.loads(str(pageSoup))

    # Decode Data
    raw_data = decode(results_json)

    return raw_data

    # # Match the formatting of player1/2 to that in the court-vision raw data's player data 
    # # If player names match their respective indexes in the court-vision raw data, 
    # # then we keep the player name order, otherwise we swap 
    # # "Truncated Name" for player 1 (e.g. R. NADAL)
    # player1_tname = player1.split(" ")[0][0]+"." + " " + player1.split(" ")[1].upper()
    # #player1_cv = raw_data['courtVisionData'][0]['a79']['a83'][0]['a85']
    # player1_cv = raw_data['players'][0]['player1Name']

    # if player1_tname == player1_cv:
    #     player1_cvfile = player1
    #     player2_cvfile = player2
    # else:
    #     player1_cvfile = player2
    #     player2_cvfile = player1

    # # Formatting
    # player1_cvfile = player1_cvfile.replace(" ","-")
    # player2_cvfile = player2_cvfile.replace(" ","-")

    # # Format the "Round Name" to appear on file path
    # # round_n = matches_scp["round"].iloc[i]
    # if "Round Of" in round_n:
    #     round_short = round_n.split(" ")[0][0] + round_n.split(" ")[-1]
    # elif round_n == '1st Round Qualifying':
    #     round_short = "Q1"
    # elif round_n == '2nd Round Qualifying':
    #     round_short = "Q2"
    # elif round_n == '3rd Round Qualifying':
    #     round_short = "Q3"
    # elif "Round" in round_n:
    #     round_short = "".join([s[0] for s in round_n.split(" ")])
    # elif round_n == "Quarterfinals" or round_n == "Quarter-Finals":
    #     round_short = "QF"
    # elif round_n == "Semifinals" or round_n == "Semi-Finals":
    #     round_short = "SF"
    # elif round_n == "Final" or round_n == "Finals":
    #     round_short = "F"

    # # # Output the decoded courtvision data into a json file
    # # with open(f"../data/court-vision/raw/{year}/{tourn_id}_{round_short}_{player1_cvfile}-vs-{player2_cvfile}_{year}_{match_id}_court-vision.json", 'w') as fp:
    # #     json.dump(raw_data, fp)
    # with open(f"../data/key-stats/raw/{year}/{tourn_id}_{round_short}_{player1_cvfile}-vs-{player2_cvfile}_{year}_{match_id}_key-stats.json", 'w') as fp:
    #     json.dump(raw_data, fp)


    # data_types = ["rally-analysis", "stroke-analysis"]
    # if matches_scp.iloc[i].court_vision == 0:
    #     continue
    # elif matches_scp.iloc[i].court_vision == 1:
    #     for i,lnk in enumerate([link_rallys, link_strokes]):
    #         # Get request and content from the given link and parse into HTML
    #         pageTree = requests.get(lnk, headers=headers)
    #         pageSoup = BeautifulSoup(pageTree.content, 'html.parser') 
    #         results_json = json.loads(str(pageSoup))
    #         # Decode Data
    #         raw_data = decode(results_json)

    #         data_type = data_types[i]

    #         # Output the decoded data into a json file
    #         with open(f"../data/{data_type}/raw/{year}/{tourn_id}_{round_short}_{player1_cvfile}-vs-{player2_cvfile}_{year}_{match_id}_{data_type}.json", 'w') as fp:
    #             json.dump(raw_data, fp)
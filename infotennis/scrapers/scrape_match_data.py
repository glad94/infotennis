"""
Scraping Functions/Methods for ATP Match Level Data
- Match Stats
- Rally Analysis
- Stroke Analysis
- Court Vision

v1 created by @glad94 15/8/2023
last tested on ATP sites on 15/8/2023

"""
import base64
import datetime
import json
import logging
import os
import sys
from time import sleep
import warnings
warnings.filterwarnings("ignore")

from bs4 import BeautifulSoup
import cryptography.hazmat.backends
import cryptography.hazmat.primitives.ciphers
import cryptography.hazmat.primitives.ciphers.algorithms
import cryptography.hazmat.primitives.ciphers.modes
import cryptography.hazmat.primitives.padding
import numpy as np
import requests
import yaml

sys._enablelegacywindowsfsencoding() #Deal with pandas problem with reading file with accents in file path i.e Alexis Sánchez, Victor Lindelöf 

headers = {'User-Agent': 
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'} 

# # Log File Settings
# # Create a new log file per month 
# logging.basicConfig(filename='test_log.log',
#                     filemode='a',
#                     format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
#                     datefmt='%H:%M:%S',
#                     level=logging.INFO,
#                     force=True)

# # Suppress "WDM INFO ====== WebDriver manager ======" messages
# os.environ['WDM_LOG_LEVEL'] = '0'
# logging.getLogger('WDM').setLevel(logging.NOTSET)

# Load config file into dict 'configs'
with open("./config.yaml", "r") as yamlfile:
    configs = yaml.safe_load(yamlfile)

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
def scrape_ATP_match_data(year, tourn_id, match_id, data_type):
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
    match_id = match_id.upper()

    try:
        link = configs['atp'][data_type] % {'year': year, 'tourn_id': tourn_id, 'match_id': match_id}
    except:
        raise ValueError("Invalid data_type argument provided.")
    
    # Get request and content from the given link and parse into HTML
    pageTree = requests.get(link, headers=headers)
    pageSoup = BeautifulSoup(pageTree.content, 'html.parser') 
    
    results_json = json.loads(str(pageSoup))

    # Decode Data
    raw_data = decode(results_json)

    return raw_data


def scrape_ATP_results_data(data_dir, data_path, df_results, data_type, create_output_path=False, overwrite=False):
    

    if not os.path.exists(data_dir):
        breakpoint() ##### DEBUGGING
        print("Output Data Directory does not exist")
        logging.error(f"Output Data Directory does not exist for saving ATP {data_type} data files.")
        return False
    
    success_N = 0 # Count no. of successful data scraped+saved
    # for i, row in df_results.iterrows():
    for i, row in df_results.iterrows():
        year, tourn_id, match_id, round_n, player1, player2, court_vision =\
        row[["year", "tournament_id", "match_id", "round", "player1_name", "player2_name", "court_vision"]]
        if match_id is None:
                logging.info(f"{year} {tourn_id} {player1}-{player2} has no data found for {data_type}!")
                continue
        data_path = data_path.replace("<data_type>", data_type).replace("<year>", str(year))
        
        if not os.path.exists(data_dir + data_path):
            if create_output_path:
                logging.info(f"Output Data Path was created for saving ATP {data_type} data files.")
                os.makedirs(data_dir + data_path)
            else:
                logging.error(f"Output Data Path does not exist for saving ATP {data_type} data files.")
                breakpoint() ##### DEBUGGING
                return False

        if data_type == "court_vision" and court_vision != 1:
            continue
        # Scrape raw data to json type
        try:
        #breakpoint()
            raw_data = scrape_ATP_match_data(year, tourn_id, match_id, data_type)

            # Output file formatting
            player1_fn = player1.replace(" ","-")
            player2_fn = player2.replace(" ","-")

            if "Round Of" in round_n:
                round_short = round_n.split(" ")[0][0] + round_n.split(" ")[-1]
            elif "Round Qualifying" in round_n:
                round_short = "Q" + round_n.split(" ")[0][0]
            elif "Round" in round_n:
                round_short = "".join([s[0] for s in round_n.split(" ")])
            elif round_n == "Quarterfinals" or round_n == "Quarter-Finals":
                round_short = "QF"
            elif round_n == "Semifinals" or round_n == "Semi-Finals":
                round_short = "SF"
            elif round_n == "Final" or round_n == "Finals":
                round_short = "F"

            # Output the decoded courtvision data into a json file
            out_file = f"{tourn_id}_{round_short}_{player1_fn}-vs-{player2_fn}_{year}_{match_id.upper()}_{data_type}.json" 
            # Skip the match if a raw json file of that type already exists, if overwrite is False
            if not overwrite and os.path.exists(data_dir + data_path + out_file):
                logging.info(f"{year} {tourn_id} {match_id} {player1}-{player2} {data_type} file already exists in {data_dir + data_path}!")
                continue
            
            with open(data_dir + data_path + out_file, 'w') as fp:
                json.dump(raw_data, fp)
            success_N += 1

            sleeptime = np.random.uniform(1, 3)
            sleep(sleeptime)

        except:
            logging.info(f"{year} {tourn_id} {match_id} {player1}-{player2} Failed or no Data found for {data_type}!")
            pass
    
    print(f"Successfully scraped and added {success_N} files to {data_dir+data_path}.")
    logging.info(f"Successfully scraped and added {success_N} files to {data_dir+data_path}/.")
    if success_N > 0:
        return True
    else:  
        return False
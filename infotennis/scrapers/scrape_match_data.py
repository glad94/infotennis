HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
"""
Scraping Functions/Methods for ATP Match Level Data (valid for Antwerp 2021 matches onwards)
- Match Stats
- Rally Analysis
- Stroke Analysis
- Court Vision

v1 created by @glad94 15/8/2023
last tested on ATP sites on 7/11/2023

Heavy lifting here is all thanks to Github/Stackoverflow user Gabjauf who provided the solution at:
https://stackoverflow.com/questions/73735401/scraping-an-api-returns-what-looks-like-encrypted-data

If the cypher method changes, then the above method will no longer work.
"""
import base64
import datetime
import json
import logging
import os
import sys
import warnings
import asyncio
import aiohttp
from aiohttp import ClientSession
from time import sleep
from functools import partial
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import numpy as np
import pandas as pd
import yaml
from bs4 import BeautifulSoup
import cryptography.hazmat.backends
import cryptography.hazmat.primitives.ciphers
import cryptography.hazmat.primitives.ciphers.algorithms
import cryptography.hazmat.primitives.ciphers.modes
import cryptography.hazmat.primitives.padding


# # Suppress "WDM INFO ====== WebDriver manager ======" messages
# os.environ['WDM_LOG_LEVEL'] = '0'
# logging.getLogger('WDM').setLevel(logging.NOTSET)

# Load config file into dict 'configs'
script_dir = os.path.dirname(__file__)
config_path = os.path.join(script_dir, "../../config.yaml")
#with open("./config.yaml", "r") as yamlfile:
with open(config_path, "r") as yamlfile:
    configs = yaml.safe_load(yamlfile)

##############################################
# Decrypting Utilities
def formatDate(t):
    """
    Returns a formatted form of the 'lastModified' key from the encrypted data object.
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
    """
    Decrypting algorithm for encrypted ATP match statistics data.

    Credit: Github/Stackoverflow user Gabjauf
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

# Async version of scrape_ATP_match_data with retry/backoff and logging
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception)
)
async def scrape_ATP_match_data_async(
    session: ClientSession,
    year: int,
    tourn_id: str,
    match_id: str,
    data_type: str,
    log_list: list
) -> None:
    match_id = match_id.upper()
    try:
        link = configs['atp'][data_type] % {'year': year, 'tourn_id': tourn_id, 'match_id': match_id}
    except Exception as e:
        raise ValueError(f"Invalid data_type argument provided. Error {e}")

    params = {'year': year, 'tourn_id': tourn_id, 'match_id': match_id, 'data_type': data_type}
    time_utc = datetime.datetime.utcnow().isoformat()
    log_entry = {"url": link, "params": params, "time_utc": time_utc, "success": False}
    try:
        async with session.get(link, headers=HEADERS, timeout=30) as resp:
            resp.raise_for_status()
            text = await resp.text()
            pageSoup = BeautifulSoup(text, 'html.parser')
            results_json = json.loads(str(pageSoup))
            raw_data = decode(results_json)
            log_entry["success"] = True
            return raw_data, log_entry
    except Exception as e:
        log_entry["success"] = False
        log_entry["error"] = str(e)
        raise
    finally:
        log_list.append(log_entry)


def scrape_ATP_results_data(
    data_dir: str,
    data_path: str,
    df_results: pd.DataFrame,
    data_type: str,
    create_output_path=False,
    overwrite=False
):
    """
    Asynchronous scraping of ATP match statistics data of the specified type and save as JSON files.
    Uses asyncio, aiohttp, semaphore (max 15), tenacity for retry/backoff, and logs each API call.
    """
    import nest_asyncio
    nest_asyncio.apply()

    if not os.path.exists(data_dir):
        print("Output Data Directory does not exist")
        logging.error(f"Output Data Directory does not exist for saving ATP {data_type} data files.")
        return False

    # Prepare tasks
    rows = df_results.to_dict(orient="records")
    semaphore = asyncio.Semaphore(15)
    log_list = []
    success_N = 0

    async def process_row(row, session):
        nonlocal success_N
        year = row["year"]
        tourn_id = row["tournament_id"]
        match_id = row["match_id"]
        round_n = row["round"]
        player1 = row["player1_name"]
        player2 = row["player2_name"]
        court_vision = row.get("court_vision", None)
        local_data_path = data_path.replace("<data_type>", data_type).replace("<year>", str(year))
        full_path = data_dir + local_data_path

        if match_id is None:
            logging.info(f"{year} {tourn_id} {player1}-{player2} has no data found for {data_type}!")
            return
        if not os.path.exists(full_path):
            if create_output_path:
                logging.info(f"Output Data Path was created for saving ATP {data_type} data files.")
                os.makedirs(full_path, exist_ok=True)
            else:
                logging.error(f"Output Data Path does not exist for saving ATP {data_type} data files.")
                return
        if data_type == "court_vision" and court_vision != 1:
            return
        player1_fn = player1.replace(" ", "-")
        player2_fn = player2.replace(" ", "-")
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
        else:
            round_short = round_n
        out_file = f"{tourn_id}_{round_short}_{player1_fn}-vs-{player2_fn}_{year}_{str(match_id).upper()}_{data_type}.json"
        out_file_path = os.path.join(full_path, out_file)
        if not overwrite and os.path.exists(out_file_path):
            logging.info(f"{year} {tourn_id} {match_id} {player1}-{player2} {data_type} file already exists in {full_path}!")
            return
        # Async scrape with semaphore
        async with semaphore:
            try:
                raw_data, log_entry = await scrape_ATP_match_data_async(session, year, tourn_id, match_id, data_type, log_list)
                with open(out_file_path, 'w') as fp:
                    json.dump(raw_data, fp)
                success_N += 1
                await asyncio.sleep(np.random.uniform(1, 3))
            except Exception as e:
                logging.info(f"{year} {tourn_id} {match_id} {player1}-{player2} Failed or no Data found for {data_type}! Error: {e}")

    async def main():
        async with aiohttp.ClientSession() as session:
            tasks = [process_row(row, session) for row in rows]
            await asyncio.gather(*tasks)

    asyncio.run(main())

    # Log all API calls
    for entry in log_list:
        logging.info(f"API_CALL_LOG: {json.dumps(entry)}")

    print(f"Successfully scraped and added {success_N} files to {data_path.replace('<data_type>', data_type)}.")
    logging.info(f"Successfully scraped and added {success_N} files to {data_path.replace('data_type>', data_type)}/.")
    if success_N > 0:
        return True
    else:
        return False

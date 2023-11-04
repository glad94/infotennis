"""
Created by Gerald on 21/9/2023 10:36 LT.

Processing functions for raw scraped data to dataframe (for DB insertion). For ATP Key-Stats.
"""
import numpy as np
import pandas as pd

def calc_percentage(ratio_str):
    """_summary_

    Args:
        ratio_str (_type_): _description_

    Returns:
        float: _description_
    """
    if ratio_str is None:
        return None
    numer = int(str(ratio_str).split(' ')[0].split("/")[0])
    denom = int(str(ratio_str).split(' ')[0].split("/")[1])

    if denom == 0:
        return 0
    else:
        return np.round(numer*100/denom,1)

def get_unret_serves(raw_data_r, set_n=0):
    '''
    Returns the number of unreturned serves for both players from a rally-analysis raw data file
    The assumption here is that the "t{player}err" column from the rallyData dataframe, at iloc[1] correctly refers to unreturned serves 
    '''

    # # Count unreturned Serves (From rally analysis data if avail)
    p1_unret_list = [ item['pointId'] for item in pd.json_normalize(raw_data_r['rallyData']).iloc[1].t2err ]
    p2_unret_list = [ item['pointId'] for item in pd.json_normalize(raw_data_r['rallyData']).iloc[1].t1err ]

    # Subset data accordingly depending on which set's data is being processed
    if set_n == 0:
        return [len(p1_unret_list), len(p2_unret_list)]
    else:
        # Split into sets
        # Since the "set number" of each point id is given by the prefix "setnum_"
        p1_unret_list_s = [x for x in p1_unret_list if x[:2] == f"{set_n}_"]
        p2_unret_list_s = [x for x in p2_unret_list if x[:2] == f"{set_n}_"]

        return [len(p1_unret_list_s), len(p2_unret_list_s)]

def process_set_stats(year, tourn_id, match_id, round_n, 
    player_ids, raw_data, set_n=0, raw_data_rallies=None):
    """
    Reads in a raw stats data dict and processes it into a Dataframe for a single provided set

    -----------------------------------
    Params:
        data_stats: dict

        set_n: int
        
    Returns:
        df_stats: pd.DataFrame
    
    """
    df_stats = pd.DataFrame(raw_data['setStats'][f'set{set_n}']).iloc[:,1:4].T
    # Return if df_stats is empty. Seen one case where 'setsCompleted' was incorrect (1 extra set) in raw_data.
    if len(df_stats) == 0:
        return 
    # Set the first row to be the column names
    df_stats.rename(columns=df_stats.iloc[0], inplace = True)
    # Drop the original first row 
    df_stats = df_stats.drop(df_stats.index[0])

    # Rename the columns to a lower case + underscore convention
    df_stats.columns =  [x.lower().replace(" ","_") for x in df_stats.columns]

    # This is the set of columns expected if the full key stats are collected
    # Some matches e.g. qualifiers at 250s don't have certain collected stats
    columns_full = ['serve_rating', 'aces', 'double_faults', '1st_serve',
        '1st_serve_points_won', '2nd_serve_points_won', 'break_points_saved',
        'service_games_played', 'return_rating', '1st_serve_return_points_won',
        '2nd_serve_return_points_won', 'break_points_converted',
        'return_games_played', 'net_points_won', 'winners', 'unforced_errors',
        'service_points_won', 'return_points_won', 'total_points_won',
        'max_speed', '1st_serve_average_speed', '2nd_serve_average_speed']

    ### 1. Reformatting existing stats/columns
    # If the raw stats file is missing certain stat fields, we reindex the columns so that it will match
    # the full set. Empty fields will be given NaN
    if len(df_stats.columns) != columns_full:
        df_stats = df_stats.reindex(columns=columns_full, fill_value="")
    # Gather list of columns to reformat into % numbers
    percen_list = ["1st_serve", "1st_serve_points_won", "2nd_serve_points_won", "break_points_saved",
    "1st_serve_return_points_won", "2nd_serve_return_points_won", "break_points_converted",
    "net_points_won", "service_points_won", "return_points_won", "total_points_won"]

    for col in percen_list: 
        col_idx = df_stats.columns.get_loc(col)
        # Insert formatted % data into as a new column
        try:
            df_stats.insert(col_idx+1, col+"_pct", df_stats[col].apply(lambda x: calc_percentage(x)))
        except ValueError as ve:
            # If valuerror encountered, e.g. due to missing stat which will be filled as ""
            df_stats.insert(col_idx+1, col+"_pct", df_stats[col].apply(lambda x: x))
        # Reformat the original column to remove the bracket enclosed % value
        df_stats[col] = df_stats[col].apply(lambda x: x.split(' ')[0])

    ### 2. Adding additional Stats
    # Get number of break points faced
    BPfaced = df_stats["break_points_saved"].apply(lambda x: str(x).split('/')[-1].split(' (')[0])
    # # Insert BP faced list into df_stats
    df_stats.insert(df_stats.columns.get_loc("break_points_converted")+2, 'break_points_faced', BPfaced)

    # If rally-analysis data is available for this match, compute the no. of unreturned serves, else fill with None
    if raw_data_rallies is not None:
        # Insert this information right after aces
        df_stats.insert(df_stats.columns.get_loc("aces")+1, 'serves_unreturned', get_unret_serves(raw_data_rallies, set_n))
    else:
        df_stats.insert(df_stats.columns.get_loc("aces")+1, 'serves_unreturned', [-999, -999])

    ### 3. Match Metadata 
    df_stats.insert(0, "year", [year]*2)
    df_stats.insert(1, "tournament_id", [str(int(tourn_id))]*2)
    df_stats.insert(2, "match_id", [match_id.lower()]*2)
    df_stats.insert(3, "round", [round_n]*2)
    df_stats.insert(4, "sets_completed", [raw_data['setsCompleted']]*2)
    df_stats.insert(5, "set_n", [set_n]*2)
    df_stats.insert(6, "player_id", player_ids)
    df_stats.insert(7, "opponent_id", player_ids[::-1])

    # Reset the index
    df_stats = df_stats.reset_index(drop=True)
    # Convert Dataframe values to numeric where appropriate, else ignore (e.g. for strings)
    df_stats = df_stats.apply(pd.to_numeric, errors='ignore')
    # Reconvert the dtype of tournament_id back to object
    df_stats["tournament_id"] = df_stats["tournament_id"].astype('Int64').astype(str)

    #Final renaming of columns to a more SQL-friendly standard
    num_cols_rnm = {"1st_serve":"serve1",\
    "1st_serve_pct": "serve1_pct",\
    "1st_serve_points_won":"serve1_pts_won",\
    "1st_serve_points_won_pct":"serve1_pts_won_pct",\
    "2nd_serve_points_won":"serve2_pts_won",\
    "2nd_serve_points_won_pct":"serve2_pts_won_pct",\
    "1st_serve_return_points_won":"serve1_return_pts_won",\
    "1st_serve_return_points_won_pct":"serve1_return_pts_won_pct",\
    "2nd_serve_return_points_won":"serve2_return_pts_won",\
    "2nd_serve_return_points_won_pct":"serve2_return_pts_won_pct",\
    "1st_serve_average_speed":"serve1_avg_speed",\
    "2nd_serve_average_speed":"serve2_avg_speed"}

    df_stats = df_stats.rename(columns = num_cols_rnm)
    return df_stats

def process_key_stats(year, tourn_id, match_id, round_n, raw_data, raw_data_rallies=None):
    """
    Returns a Dataframe containing the processed stats for the given match (identified by the year, tourn_id and match_id)
    
    """
    ### Get some info from raw_data
    # No. of sets played
    n_sets = raw_data['setsCompleted']
    # Get stats player IDs
    player_ids = list(pd.DataFrame(raw_data['players']).player1Id)
    
    ### Actual data processing starts
    df_stats_list = []
    # Loop through each played set and process the stats into a DF, append each to the list of DFs and then concat 
    for n in range(n_sets+1):
        df_stats_n = process_set_stats(year, tourn_id, match_id, round_n, player_ids, raw_data, n, raw_data_rallies)
        # Append to list
        df_stats_list.append(df_stats_n)

    df_stats = pd.concat(df_stats_list)

    # Do some sanity-based data cleaning.
    # Have observed nonsensical serve-speed values before. Use 300 as a max-speed filter (world record is like 260-269 kmh)
    for col_spd in ["max_speed", "serve1_avg_speed", "serve2_avg_speed"]:
        df_stats.loc[abs(df_stats[col_spd]) > 300, col_spd] = -999

    return df_stats
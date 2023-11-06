"""
Created by Gerald on 21/9/2023 10:40 LT.

Processing functions for raw scraped data to dataframe (for DB insertion). For ATP Rally Analysis.
"""
import numpy as np
import pandas as pd

def process_rallystat_col(rally_df_r: pd.DataFrame, shot_num: int, outcome: str, player_id: str, opp_id: str):
    """Process and extract rally statistics from raw rally-analysis data for a specific shot outcome.

    Args:
        rally_df_r (pandas.DataFrame): The raw rally-analysis data for a specific outcome.
        shot_num (int): The shot number (1 to 11), where 9 represents 9+ (odd), 10 represents
        10+ (even), and 11 is uncategorized.
        outcome (str): The shot outcome, one of {"t1err", "t1win", "t2err", "t2win"} (from rally_df_r).
        player_id (str): The ID of the player associated with the outcome.
        opp_id (str): The ID of the opponent player.

    Returns:
        rally_sub_df (pandas.DataFrame): A processed DataFrame containing rally statistics for the specified shot outcome.

        It also checks for any incorrectly assigned rows, such as "DOUBLE FAULT" appearing in the wrong outcome category
        and corrects them if needed. (Not sure how exhaustive this is.)
    """

    rally_sub_df = pd.json_normalize(rally_df_r[outcome].iloc[shot_num-1])

    if len(rally_sub_df) == 0:
        return rally_sub_df

    # Convert raw column names from camel to snake case     
    rally_sub_df.columns = rally_sub_df.columns.str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)\
        .str.replace('^t(1|2)', r'p\1_', regex=True).str.lower()

    if "err" in outcome:
        shot_outcome = "L"
    elif "win" in outcome:
        shot_outcome = "W"

    if shot_num < 9:
        shot_num = str(int(shot_num))
    elif shot_num == 9:
        shot_num = "9+_odd"
    elif shot_num == 10:
        shot_num = "10+_even"
    else:
        shot_num = "Unknown"
        
    ### Match Metadata 
    rally_sub_df.insert(0, "shot_number", [shot_num]*len(rally_sub_df))
    rally_sub_df.insert(1, "outcome", [shot_outcome]*len(rally_sub_df))
    rally_sub_df.insert(2, "player_id", [player_id]*len(rally_sub_df))
    rally_sub_df.insert(3, "opponent_id", [opp_id]*len(rally_sub_df))

    # Check for any incorrectly assigned rows, e.g. DOUBLE FAULT appears in "t1win" which shd belong in "t2err"
    err_indexs = np.where((rally_sub_df.point_end_type == "DOUBLE FAULT") & (rally_sub_df.outcome == "W"))[0]
    for ei in err_indexs:
        rally_sub_df.loc[ei,["outcome", "player_id", "opponent_id"]] = ["L", opp_id, player_id]

    return rally_sub_df

def process_rally_analysis(year: int, tourn_id: str, match_id: str, round_n: str, raw_data: dict):
    """
    Reads in raw rally-analysis data and returns a dataFrame with rally-type data (shot_number, 
    point_end_type, serve, serve_speed, etc.) sorted by point played.

    Args:
        year (int): Year in which the match took place (e.g. 2023).
        tourn_id (str): Tournament ID of the match (e.g. "404" - Indian Wells).
        match_id (str): Match ID of the match (e.g. "ms001").
        round_n (str): Round in which the match took place (e.g. "Final").
        raw_data (dict): Raw rally-analysis data (from JSON).

    Returns:
        df_rallies (pandas.DataFrame): Processed rally-analysis dataframe.
    """

    # Get stats player IDs
    player_ids = list(pd.DataFrame(raw_data['playerDetails']).player1Id)
    
    ### Actual data processing starts
    list_rally_sub_df = []
    for outcome in ["t1win", "t1err"]:
        df_rallies_r = pd.json_normalize(raw_data['rallyData'])
        rally_sub_df1 = pd.concat([process_rallystat_col(df_rallies_r, i+1, outcome, player_ids[0], player_ids[1]) for i in range(len(df_rallies_r))] )
        list_rally_sub_df.append(rally_sub_df1)
    for outcome in ["t2win", "t2err"]:
        rally_sub_df2 = pd.concat([process_rallystat_col(df_rallies_r, i+1, outcome, player_ids[1], player_ids[0]) for i in range(len(df_rallies_r))] )
        list_rally_sub_df.append(rally_sub_df2)
    
    df_rallies = pd.concat(list_rally_sub_df).reset_index(drop=True)

    # Insert game and point columns
    idx_set = df_rallies.columns.get_loc('set') #Get index of the "set" column, then insert "game" and "point" after it
    df_rallies.insert(idx_set+1, "game", df_rallies.point_id.apply(lambda x: x.split("_")[1]))
    df_rallies.insert(idx_set+2, "point", df_rallies.point_id.apply(lambda x: x.split("_")[2]))

    # Order the dataframe
    df_rallies= df_rallies.sort_values(["set", "game", "point","serve"]).reset_index(drop=True)
    # Insert generic cols
    df_rallies.insert(0, "year", [year]*len(df_rallies))
    df_rallies.insert(1, "tournament_id", [str(int(tourn_id))]*len(df_rallies))
    df_rallies.insert(2, "match_id", [match_id.lower()]*len(df_rallies))
    df_rallies.insert(3, "round", [round_n]*len(df_rallies))
    df_rallies.insert(4, "sets_completed", [raw_data['setsCompleted']]*len(df_rallies))

    df_rallies = df_rallies.rename(columns = {"set":"set_n"})

    return df_rallies
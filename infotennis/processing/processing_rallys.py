"""
Created by Gerald on 21/9/2023 10:40 LT.

Processing functions for raw scraped data to dataframe (for DB insertion). For ATP Rally Analysis.
"""
import numpy as np
import pandas as pd

def process_rallystat_col(rally_df_r, shot_num, outcome, player_id, opp_id):
    """Process the t1err/t1win/t2err/t2win columns from the raw rally-analysis data

    Args:
        rally_df_r (_type_): _description_
        shot_num (int): Number from 1 to 11, where 9: 9+ (Odd), 10: 10+ (Even), 11 (Uncategorised)
        outcome (str): A column from rally_df_r {"t1err", "t1win", "t2err", "t2win"}
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

def process_rally_analysis(year, tourn_id, match_id, round_n, raw_data):

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
"""
Created by Gerald on 21/9/2023 10:42 LT.

Processing functions for raw scraped data to dataframe (for DB insertion). For ATP Stroke-Analysis.
"""
import pandas as pd

def process_stroke_set(player_ids, df_strokes_r, n):
    """_summary_

    Args:
        player_ids (_type_): _description_
        df_strokes_r (_type_): _description_
        n (_type_): _description_

    Returns:
        _type_: _description_
    """
    list_df_strokes = [] 
    for player in ["player1", "player2"]:
        for hand in ["forehand", "backhand"]:
            df_H_p_s0 = pd.json_normalize(df_strokes_r[hand].iloc[n]).filter(regex=player).iloc[:,1:5]
            df_H_p_s0 = df_H_p_s0.rename(columns = {f"{player}":"set_n", f"{player}Wins":"winners", f"{player}Frcs":"errors", f"{player}Unfs":"unforced_errors", f"{player}Others":"others"})
            df_H_p_s0.insert(0, "shot_type", pd.json_normalize(df_strokes_r[hand].iloc[0]).name)
            df_H_p_s0.insert(0, "hand", [hand]*len(df_H_p_s0))
            if player == "player1":
                pid = player_ids[0]
                oid = player_ids[1]
            else:
                pid = player_ids[1]
                oid = player_ids[0]
            df_H_p_s0.insert(0, "player_id", [pid]*len(df_H_p_s0))
            df_H_p_s0.insert(1, "opponent_id", [oid]*len(df_H_p_s0))
            df_H_p_s0.insert(0, "set_n", [n]*len(df_H_p_s0))

            list_df_strokes.append(df_H_p_s0)

    return pd.concat(list_df_strokes).reset_index(drop=True)

def process_stroke_analysis(year, tourn_id, match_id, round_n, raw_data):
    """_summary_

    Args:
        year (_type_): _description_
        tourn_id (_type_): _description_
        match_id (_type_): _description_
        round_n (_type_): _description_
        raw_data (_type_): _description_

    Returns:
        _type_: _description_
    """
    ### Get some info from raw_data
    # No. of sets played
    n_sets = raw_data['setsCompleted']

    # Get stats player IDs
    player_ids = list(pd.DataFrame(raw_data['players']).player1Id)
    
    ### Actual data processing starts
    df_stats_list = []
    df_strokes_r = pd.json_normalize(raw_data['rallyShots']['allPoints'])
    # Check for any errors in n_sets
    if n_sets != len(df_strokes_r)-1:
        n_sets = len(df_strokes_r)-1
    # Loop through each played set and process the stats into a DF, append each to the list of DFs and then concat 
    for n in range(n_sets+1):
        df_stats_n = process_stroke_set(player_ids, df_strokes_r, n)
        # Append to list
        df_stats_list.append(df_stats_n)

    df_stats = pd.concat(df_stats_list)

    ### 3. Match Metadata 
    df_stats.insert(0, "year", [year]*len(df_stats))
    df_stats.insert(1, "tournament_id", [str(int(tourn_id))]*len(df_stats))
    df_stats.insert(2, "match_id", [match_id.lower()]*len(df_stats))
    df_stats.insert(3, "round", [round_n]*len(df_stats))
    df_stats.insert(4, "sets_completed", [n_sets]*len(df_stats))

    return df_stats
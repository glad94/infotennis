"""
Created by Gerald on 21/9/2023 10:42 LT.

Processing functions for raw scraped data to dataframe (for DB insertion). For ATP Stroke-Analysis.
"""
import pandas as pd

def process_stroke_set(player_ids: list, df_strokes_r: pd.DataFrame, set_n: int):
    """_summary_

    Args:
        player_ids (list): List of str-type player IDs [player1Id, player2Id].
        df_strokes_r (pandas.DataFrame): The raw stroke analysis data for a specific set.
        set_n (int): The set number to process.

    Returns:
        pandas.DataFrame: A processed DataFrame containing stroke statistics for the specified set.
    """
    list_df_strokes = [] 
    for player in ["player1", "player2"]:
        for hand in ["forehand", "backhand"]:
            df_hand_p_set = pd.json_normalize(df_strokes_r[hand].iloc[set_n]).filter(regex=player).iloc[:,1:5]
            df_hand_p_set = df_hand_p_set.rename(columns = {f"{player}":"set_n", f"{player}Wins":"winners", f"{player}Frcs":"errors", f"{player}Unfs":"unforced_errors", f"{player}Others":"others"})
            df_hand_p_set.insert(0, "shot_type", pd.json_normalize(df_strokes_r[hand].iloc[0]).name)
            df_hand_p_set.insert(0, "hand", [hand]*len(df_hand_p_set))
            if player == "player1":
                pid = player_ids[0]
                oid = player_ids[1]
            else:
                pid = player_ids[1]
                oid = player_ids[0]
            df_hand_p_set.insert(0, "player_id", [pid]*len(df_hand_p_set))
            df_hand_p_set.insert(1, "opponent_id", [oid]*len(df_hand_p_set))
            df_hand_p_set.insert(0, "set_n", [set_n]*len(df_hand_p_set))

            list_df_strokes.append(df_hand_p_set)

    return pd.concat(list_df_strokes).reset_index(drop=True)

def process_stroke_analysis(year: int, tourn_id: str, match_id: str, round_n: str, raw_data: dict):
    """
    Reads in raw stroke-analysis data and returns a dataFrame with stroke-type data (hand, 
    shot_type) and outcomes (winners, errors, unforced errors) per player and set played.

    Args:
        year (int): Year in which the match took place (e.g. 2023).
        tourn_id (str): Tournament ID of the match (e.g. "404" - Indian Wells).
        match_id (str): Match ID of the match (e.g. "ms001").
        round_n (str): Round in which the match took place (e.g. "Final").
        raw_data (dict): Raw stroke-analysis data (from JSON).

    Returns:
        df_strokes (pandas.DataFrame): Processed stroke-analysis dataframe.
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
    for set_n in range(n_sets+1):
        df_stats_n = process_stroke_set(player_ids, df_strokes_r, set_n)
        # Append to list
        df_stats_list.append(df_stats_n)

    df_strokes = pd.concat(df_stats_list)

    ### 3. Match Metadata 
    df_strokes.insert(0, "year", [year]*len(df_strokes))
    df_strokes.insert(1, "tournament_id", [str(int(tourn_id))]*len(df_strokes))
    df_strokes.insert(2, "match_id", [match_id.lower()]*len(df_strokes))
    df_strokes.insert(3, "round", [round_n]*len(df_strokes))
    df_strokes.insert(4, "sets_completed", [n_sets]*len(df_strokes))

    return df_strokes
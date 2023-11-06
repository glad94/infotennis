"""
Created by Gerald on 23/9/2023 16:40 LT.

Processing functions for raw scraped data to dataframe (for DB insertion). For ATP Court Vision.
"""
import numpy as np
import pandas as pd

# Court Vision raw data columns have been anonymised. The following 2 dicts maps suggested names to each column present 
# in the raw data. (Credit: petertea96)
dict_cols = {
    'a11': "cruciality",
    "a89": 'return_placement',
    "a12": 'trajectory_data',
    'a90': 'error_type',
    'a91': 'winner_placement',
    'a92': 'unforced_error_placement',
    'a81': 'point_id',
    'a13': 'server_id',
    'a14': 'scorer_id',
    'a15': 'receiver_id',
    'a16': 'ball_speed',
    'a17': 'return_speed',
    'a18': 'return_speed_kmh',
    'a93': 'rally_length',
    'a94': 'rally_length_werr',
    'a19': 'spin',
    'a20': 'height_above_net',
    'a21': 'ball_speed_kmh',
    'a22': 'height_above_net_m',
    'a23': 'distance_outside_court',
    'a24': 'distance_outside_court_m',
    'a95': 'point_end_type',
    'a25': 'stroke_type',
    'a96': 'serve_type',
    'a97': 'court',
    'a98': 'set_n',
    'a99': 'set',
    'a100': 'game',
    'a101': 'point',
    'a102': 'serve',
    'a103': 'hand',
    'a104': 'break_point',
    'a26':  'run_around_forehand',
    'a105': 'break_point_converted',
    'a106': 'trapped_by_net',
    'a27': 'ball_hit_coordinate',
    'a28': 'ball_peak_coordinate',
    'a29': 'ball_net_coordinate',
    'a30': 'ball_bounce_coordinate',
    'a31': 'ball_last_coordinate' ,
    'a32': 'server_coordinate',
    'a33': 'receiver_coordinate' ,
    'a34': 'serve_bounce_coordinate',
    'a35': 'match_score'
}

matchScore_cols = {'a122': 'p1_set1_score',
'a123': 'p1_set2_score',
'a124': 'p1_set3_score',
'a125': 'p1_set4_score',
'a126': 'p1_set5_score',
'a127': 'p1_set1_tb_score',
'a128': 'p1_set2_tb_score',
'a129': 'p1_set3_tb_score',
'a130': 'p1_set4_tb_score',
'a131': 'p1_set5_tb_score',
'a132': 'p2_set1_score',
'a133': 'p2_set2_score',
'a134': 'p2_set3_score',
'a135': 'p2_set4_score',
'a136': 'p2_set5_score',
'a137': 'p2_set1_tb_score',
'a138': 'p2_set2_tb_score',
'a139': 'p2_set3_tb_score',
'a140': 'p2_set4_tb_score',
'a141': 'p2_set5_tb_score',
'a142': 'p1_game_score',
'a143': 'p2_game_score'}

# Processing Functions
def process_points_data(year: int, tourn_id: str, match_id: str, round_n: str, raw_data: pd.DataFrame):
    """
    1st step of processing raw court vision data scraped from the ATP, AO and RG infosys sites.
    Top-Level JSON dict key renaming and key/column renaming for majority of elements under the 'pointsData' key. 

    Args:
        year (int): Year in which the match took place (e.g. 2023).
        tourn_id (str): Tournament ID of the match (e.g. "404" - Indian Wells).
        match_id (str): Match ID of the match (e.g. "ms001").
        round_n (str): Round in which the match took place (e.g. "Final").
        raw_data (pandas.core.frame.DataFrame): Dataframe of raw court vision data.

    Returns:
        df_points_sorted (pandas.core.frame.DataFrame): Intermediate processed court vision data 
        with named columns and sorted by point occurrence.
    """
    data_dict = raw_data['courtVisionData'][0]

    new_keys = ['is_match_complete', 'event_type', 'court_name', 'court_id', 'points_data', 'players_data', 'stats_data', 'sets_completed', 'point_id', 'match_status']

    for i,key in enumerate(list(data_dict.keys())):
        data_dict[new_keys[i]] = data_dict.pop(key)

    point_keys = data_dict['points_data'].keys()
    df_points_list = [pd.json_normalize(data_dict['points_data'][pkey], max_level=0) for pkey in point_keys ]

    df_points = pd.concat(df_points_list)

    # Rename all first level columns in df_points
    df_points = df_points.rename(columns=dict_cols)

    # Rename the columns of the tracking data coordinates
    df_points['trajectory_data'] = df_points['trajectory_data'].apply(lambda x: pd.DataFrame(x).rename(columns={"a70": "x", "a71": "y", "a72": "z", "a73": "position"}).to_dict('records') )

    # Rename the x,y,z coords in the following columns
    for key in ['ball_hit_coordinate', 'ball_peak_coordinate', 'ball_net_coordinate','ball_bounce_coordinate','ball_last_coordinate',\
                'server_coordinate','receiver_coordinate','serve_bounce_coordinate']:
        df_points[key] = df_points[key].apply(lambda x: pd.DataFrame([x]).rename(columns={"a70": "x", "a71": "y", "a72": "z", "a74": "erroneous_ball"}).to_dict('records')[0] )
    
    # Rename the keys in the matchScore column
    df_points['match_score'] = df_points['match_score'].apply(lambda x: pd.DataFrame([x]).rename(columns=matchScore_cols).to_dict('records')[0] )

    # Set these columns as int type so that their values will be sorted numerically rather than as strings
    cols_toset2int = ["rally_length", "rally_length_werr", "set_n", "set", "game", "point","serve"]
    df_points = df_points[~(df_points[cols_toset2int] == "NA").any(axis=1)] # Remove any rows with "NA" entry here so that the dtype can be changed to int
    for col in cols_toset2int:
        df_points[col] = df_points[col].astype(int)

    # Set these columns as float type
    float_cols = ['ball_speed', 'return_speed', 'return_speed_kmh', 'spin', 'height_above_net',\
        'ball_speed_kmh', 'height_above_net_m', 'distance_outside_court', 'distance_outside_court_m']

    for col in float_cols:
        df_points[col] = pd.to_numeric(df_points[col].apply(lambda x: x.split(" ")[0]), errors="coerce")

    df_points_sorted = df_points.sort_values(["set", "game", "point","serve"]).reset_index(drop=True)

    # Assign a player1_id and player2_id that matches up with the player1 and 2 in the court-vision data e.g. in matchScore
    player_ids = [raw_data['courtVisionData'][0]['players_data'][k][0]['a86'] for k in raw_data['courtVisionData'][0]['players_data'].keys()]

    ### 3. Match Metadata 
    df_points_sorted.insert(0, "year", [year]*len(df_points_sorted))
    df_points_sorted.insert(1, "tournament_id", [str(int(tourn_id))]*len(df_points_sorted))
    df_points_sorted.insert(2, "match_id", [match_id.lower()]*len(df_points_sorted))
    df_points_sorted.insert(3, "round", [round_n]*len(df_points_sorted))
    df_points_sorted.insert(4, "p1_id", [player_ids[0]]*len(df_points_sorted))
    df_points_sorted.insert(5, "p2_id", [player_ids[1]]*len(df_points_sorted))

    return df_points_sorted

# Processing functions for the renamed "trajectory_data" column present in the df_points_sorted returned by process_points_data()
def save_trajectory_data_one_rally(one_point_sequence):
    """
    Takes an input dictionary containing trajectory data sequence of a single point, and returns
    a dataframe of each recorded coordinate sorted by stroke and trajectory order. 

    Credit: Taken from petertea96's Github Repo, with minor edits.

    Args:
        one_point_sequence (dict or pandas.core.series.Series): Dictionary/Series containing the ball trajectory coordinates for a single point 
        derived directly from the raw data.

    Returns:
        df_ball_trajectory (pandas.core.frame.DataFrame): Dataframe containing the trajectory data sequence
        for one point. Columns are: 
            x, y, z, position, stroke_idx, point_id, set_n, game, point, serve
    """
    df_ball_trajectory = pd.DataFrame(one_point_sequence['trajectory_data'])
    # If trajectory data is missing, just return a DF with dummy -999 values for x,y,z
    if df_ball_trajectory.empty:
        df_ball_trajectory = pd.DataFrame(columns=["x", "y", "z", "position", "stroke_idx", "point_id", "set_n", "game", "point", "serve"])
        df_ball_trajectory.loc[0] = [-999, -999, -999, "hit", 1, one_point_sequence['point_id'], one_point_sequence['set_n'], \
                                    one_point_sequence['game'], one_point_sequence['point'], one_point_sequence['serve']]
        return df_ball_trajectory
    #######################################################################
    #                     Match situation information                     #
    #######################################################################
    # --> Get indices where ball is hit 
    hit_indices = df_ball_trajectory.index[df_ball_trajectory['position'] == 'hit'].tolist()
    hit_indices.append(df_ball_trajectory.shape[0])

    # Get lengths of rally index (expect 4 or 5)
    # In the usual case, we expect this sequence: Hit --> Peak --> Net --> Bounce
    # But what if it's a half volley? (Hit --> Peak --> Net)
    # But what if it's a hit on the rise?  Hit --> Peak --> Net --> Bounce --> Peak
    # *** Ball trajectory also includes erroneous balls (mishits)...so we sometimes get strike_index = 1 + rally_index
    hit_indices_diff_len = [x - hit_indices[i - 1] for i, x in enumerate(hit_indices)][1:]

    rally_length = len(hit_indices_diff_len)

    rally_index_list = []
    for rally_ind in range(1, rally_length + 1):
        rally_index_list.append(np.repeat( rally_ind, repeats=hit_indices_diff_len[rally_ind-1]))
    
    # Combine a list of numpy arrays into a single array
    df_ball_trajectory['stroke_idx'] = np.concatenate( rally_index_list, axis=0 )
    
    ##################################################
    #          Match situation information           #
    ##################################################
    df_ball_trajectory['point_id'] = one_point_sequence['point_id']
    df_ball_trajectory['set_n'] = one_point_sequence['set_n']
    df_ball_trajectory['game'] = one_point_sequence['game'] 
    df_ball_trajectory['point'] = one_point_sequence['point']
    df_ball_trajectory['serve'] = one_point_sequence['serve']
    
    return df_ball_trajectory

# Create list of columns to be used for storing the ball coords in the processed data (wide format)
cols_ordered = []
traj_cols = ["hit", "peak_pre", "net", "bounce", "peak_post"]
for col in traj_cols:
    cols_ordered += [c + col  for c in ["x_", "y_", "z_"]]

def process_stroke_trajectory(df_stroke_trajectory: pd.DataFrame):
    """
    Converts the ball trajectory dataframe for 1 stroke (i.e. 1 stroke_idx) from long to wide format.

    Args:
        df_stroke_trajectory (pandas.core.frame.DataFrame): Dataframe containing the trajectory data sequence
        for one stroke (e.g. subset of data from df_ball_trajectory that belongs to 1 stroke_idx).

    Returns:
        df_stroke_trajectory_wide (pandas.core.frame.DataFrame): Dataframe containing the trajectory data sequence
        for one stroke as a single row (wide-format). Columns are: 
            stroke_idx, point_id, set_n, game, point, serve, x_hit, y_hit, z_hit,...(other trajectory xyz coords)
        
        where the available trajectory suffixes are:
            hit:        where the ball contacts a racket
            peak_pre:   vertical peak of the ball pre-bounce or next hit
            net:        where the ball crosses the net (i.e. x=0)
            bounce:     where the ball bounces on the court (i.e. z=0)
            peak_post:  vertical peak of the ball post-bounce (if it did)
    """
    idx_peaks = df_stroke_trajectory.index[df_stroke_trajectory.loc[:,"position"] == "peak"]
    if len(idx_peaks) > 0:
        df_stroke_trajectory.loc[idx_peaks[0], "position"] = 'peak_pre' #Trajectory peak pre-bounce (can be before or after crossing the net)
    if len(idx_peaks) > 1:
        df_stroke_trajectory.loc[idx_peaks[1], "position"] = 'peak_post' #Trajectory peak post-bounce

    df_stroke_trajectory_wide = df_stroke_trajectory.pivot_table(index=['stroke_idx', 'point_id', 'set_n', 'game', 'point', 'serve'], columns='position', values=['x', 'y', 'z'], aggfunc='first')
    # Flatten the multi-index columns
    df_stroke_trajectory_wide.columns = [f'{col}_{pos}' for col, pos in df_stroke_trajectory_wide.columns]

    # Reset the index
    df_stroke_trajectory_wide.reset_index(inplace=True)
    # Insert missing columns
    for col in cols_ordered:
    #    insert_missing_traj_col(df_stroke_wide, col)
        if col not in df_stroke_trajectory_wide.columns:
            df_stroke_trajectory_wide[col] = -999

    df_stroke_trajectory_wide = df_stroke_trajectory_wide[list(df_stroke_trajectory_wide.columns[:6]) + cols_ordered]

    return df_stroke_trajectory_wide

def process_point_trajectory(df_ball_trajectory: pd.DataFrame):
    """
    Concats a list of process_stroke_trajectory() calls on df_stroke_trajectory within a point to return
    a Dataframe of processed ball trajectory data for one point in a wide format.

    Args:
        df_ball_trajectory (pandas.core.frame.DataFrame): Dataframe of ball trajectory data, i.e. returned
        from save_trajectory_data_one_rally().

    Returns:
        df_point_trajectory (pandas.core.frame.DataFrame): Dataframe of processed ball trajectory data for one point in
        a wide format. See help(process_stroke_trajectory) for dataframe columns and explanation.
    """
    pt_len = df_ball_trajectory.stroke_idx.max()
    df_point_trajectory_wide = pd.concat([process_stroke_trajectory(df_ball_trajectory[df_ball_trajectory.stroke_idx==i]) for i in np.arange(1,pt_len+1)])
    return df_point_trajectory_wide
    

# Processing functions for the renamed "match_score" column present in the df_points_sorted returned by process_points_data()
def process_point_score(df_point_sorted: pd.DataFrame, setend_point_ids: list, tourn_id: str):
    """
    Returns a processed dataframe of the current match score for a given point from the intermediate
    court vision data, which additionally derives the number of sets won per player and if the point was
    played during a tie-break.

    Args:
        df_point_sorted (pandas.core.series.Series): A single row of intermediate processed court vision data
        from process_points_data().
        setend_point_ids (list): List of point_ids of the match's converted set points.
        tourn_id (str): Tournament ID of the match. Used to check for tie-break triggering format.

    Returns:
        df_point_score_processed (pandas.core.frame.DataFrame): Processed dataframe of the current match score for a given point from the intermediate
    court vision data. Columns are: 
        p1_sets_w, p2_sets_w, p1_set_score, p2_set_score, p1_game_score, p2_game_score, is_tiebreak
    """
    df_point_score = df_point_sorted.match_score
    set_n = df_point_sorted.set_n

    # Compute how many sets each player has won at the current point
    p1_sets_w = 0
    p2_sets_w = 0
    if set_n != 1:
        for s in range(set_n-1, 0, -1):
            try:
                if df_point_score[f'p1_set{s}_score'] > df_point_score[f'p2_set{s}_score']:
                    p1_sets_w += 1
                elif df_point_score[f'p1_set{s}_score'] < df_point_score[f'p2_set{s}_score']:    
                    p2_sets_w += 1
            except: # Just none if there are missing data and the set_scores are NoneType, interrupting the >/< comparison
                p1_sets_w = -999
                p2_sets_w = -999
                continue
    
    p1_set_score = df_point_score[f'p1_set{set_n}_score']
    p2_set_score = df_point_score[f'p2_set{set_n}_score']

    if p1_set_score is None:
        p1_set_score = -999
    if p2_set_score is None:
        p2_set_score = -999

    # For some reason the supposedly tb_score cols don't represent anything useful (by eye)
    # if df_point_score[f'p1_set{set_n}_tb_score'] == "0" and df_point_score[f'p2_set{set_n}_tb_score'] == "0":
    #     p1_game_score = df_point_score['p1_game_score']
    #     p2_game_score = df_point_score['p2_game_score']
    #     is_tiebreak = 0
    # else:
    #     p1_game_score = df_point_score['p1_set1_tb_score']
    #     p2_game_score = df_point_score['p2_set1_tb_score']
    #     is_tiebreak = 1
    p1_game_score = df_point_score['p1_game_score']
    p2_game_score = df_point_score['p2_game_score']

    # Tiebreak determination
    if tourn_id == '7696': # Tiebreak at 3-3 only for Nextgen Finals
        score_tb = '3' # Set score both players must have for there to be a TB
        score_tbW = '4'
    else:
        score_tb = '6'
        score_tbW = '7'

    if (df_point_score[f'p1_set{set_n}_score'] == score_tbW and df_point_score[f'p2_set{set_n}_score'] == score_tb) or \
        (df_point_score[f'p1_set{set_n}_score'] == score_tb and df_point_score[f'p2_set{set_n}_score'] == score_tbW):
        is_tiebreak = 1
    elif df_point_score[f'p1_set{set_n}_score'] == score_tb and df_point_score[f'p2_set{set_n}_score'] == score_tb:
        if p1_game_score != "GAME" and p2_game_score != "GAME": # If there is a "GAME", it's from the last point that just precedes the TB
            is_tiebreak = 1
        else:
            is_tiebreak = 0
    else:
        is_tiebreak = 0

    # Add an additional set to sets_w if the point is the last point of the match 
    if df_point_sorted.point_id in setend_point_ids:
        if p1_game_score == "GAME":
            p1_sets_w += 1
        else:
            p2_sets_w += 1

    df_point_score_processed = {"p1_sets_w": p1_sets_w, "p2_sets_w": p2_sets_w, "p1_set_score": int(p1_set_score), "p2_set_score": int(p2_set_score), \
            "p1_game_score": p1_game_score, "p2_game_score": p2_game_score, "is_tiebreak": is_tiebreak}
    
    return df_point_score_processed

# Put all above functions in sequence to process from raw data -> dataframe for atp_court_vision table
def process_court_vision(year: int, tourn_id: str, match_id: str, round_n: str, raw_data: pd.DataFrame):
    """_summary_

    Args:
        year (int): Year in which the match took place (e.g. 2023).
        tourn_id (str): Tournament ID of the match (e.g. "404" - Indian Wells).
        match_id (str): Match ID of the match (e.g. "ms001").
        round_n (str): Round in which the match took place (e.g. "Final").
        raw_data (pandas.core.frame.DataFrame): Dataframe of raw court vision data.

    Returns:
        df_court_vision (pandas.core.frame.DataFrame): Final processed court vision data, with 1 row
        per shot-stroke containing ball trajectory coordinates and match score at the given stroke's point.
    """
    df_points_sorted = process_points_data(year, tourn_id, match_id, round_n, raw_data)
    # Return list of the row indexes of each set's last point
    setend_point_ids = df_points_sorted.groupby("set_n").last().point_id.tolist()
    # Return DF of all points' trajectories for the serve, return, 3rd shot and last shot
    df_trajectories_all = pd.concat( [ process_point_trajectory(save_trajectory_data_one_rally(df_points_sorted.iloc[i])) for i in range(len(df_points_sorted)) ] )
    # Return DF of the match score for every row in df_points_sorted
    df_match_score = pd.DataFrame([process_point_score(df_points_sorted.iloc[x], setend_point_ids, tourn_id) for x in range(len(df_points_sorted))])
    df_points_sorted_shortn = df_points_sorted[["year", "tournament_id", "match_id", "round", "p1_id", "p2_id", "point_id", "server_id", "scorer_id", "receiver_id",\
                                                "ball_speed_kmh", "rally_length", 'point_end_type', 'stroke_type', 'serve_type', 'court', 'set_n',\
                                                'game', 'point', 'serve', 'hand', 'break_point','break_point_converted']]
    # Concat, and merge to create final processed DF
    df_court_vision = pd.merge(pd.concat([df_points_sorted_shortn, df_match_score], axis=1), df_trajectories_all, on=["point_id", "set_n", "game", "point", "serve"])

    return df_court_vision
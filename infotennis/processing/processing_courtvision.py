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
def process_points_data(year, tourn_id, match_id, round_n, raw_data):
    """
    Data processing of the raw court vision data scraped from the ATP, AO and RG infosys sites.
    Top-Level JSON dict key renaming and key/column renaming for majority of elements under the 'pointsData' key. 
    
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
    '''
    Args:
    -----
    one_point_sequence [dict]: Dictionary
    
    Returns:
    --------
    pandas DataFrame (for one point sequence)
    
    Notes: Taken from petertea96's Github Repo, with minor edits.
    ------
    '''
    ball_trajectory_df = pd.DataFrame(one_point_sequence['trajectory_data'])
    # If trajectory data is missing, just return a DF with dummy -999 values for x,y,z
    if ball_trajectory_df.empty:
        ball_trajectory_df = pd.DataFrame(columns=["x", "y", "z", "position", "stroke_idx", "point_id", "set_n", "game", "point", "serve"])
        ball_trajectory_df.loc[0] = [-999, -999, -999, "hit", 1, one_point_sequence['point_id'], one_point_sequence['set_n'], \
                                    one_point_sequence['game'], one_point_sequence['point'], one_point_sequence['serve']]
        return ball_trajectory_df
    #######################################################################
    #                     Match situation information                     #
    #######################################################################
    # --> Get indices where ball is hit 
    hit_indices = ball_trajectory_df.index[ball_trajectory_df['position'] == 'hit'].tolist()
    hit_indices.append(ball_trajectory_df.shape[0])

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
    ball_trajectory_df['stroke_idx'] = np.concatenate( rally_index_list, axis=0 )
    
    ##################################################
    #          Match situation information           #
    ##################################################
    ball_trajectory_df['point_id'] = one_point_sequence['point_id']
    ball_trajectory_df['set_n'] = one_point_sequence['set_n']
    ball_trajectory_df['game'] = one_point_sequence['game'] 
    ball_trajectory_df['point'] = one_point_sequence['point']
    ball_trajectory_df['serve'] = one_point_sequence['serve']
    
    return ball_trajectory_df


cols_ordered = []
traj_cols = ["hit", "peak_pre", "net", "bounce", "peak_post"]
for col in traj_cols:
    cols_ordered += [c + col  for c in ["x_", "y_", "z_"]]

def process_stroke_trajectory(df_stroke):

    idx_peaks = df_stroke.index[df_stroke.loc[:,"position"] == "peak"]
    if len(idx_peaks) > 0:
        df_stroke.loc[idx_peaks[0], "position"] = 'peak_pre' #Trajectory peak pre-bounce (can be before or after crossing the net)
    if len(idx_peaks) > 1:
        df_stroke.loc[idx_peaks[1], "position"] = 'peak_post' #Trajectory peak post-bounce

    pivot_df = df_stroke.pivot_table(index=['stroke_idx', 'point_id', 'set_n', 'game', 'point', 'serve'], columns='position', values=['x', 'y', 'z'], aggfunc='first')
    # Flatten the multi-index columns
    pivot_df.columns = [f'{col}_{pos}' for col, pos in pivot_df.columns]

    # Reset the index
    pivot_df.reset_index(inplace=True)
    # Insert missing columns
    for col in cols_ordered:
    #    insert_missing_traj_col(pivot_df, col)
        if col not in pivot_df.columns:
            pivot_df[col] = -999

    pivot_df = pivot_df[list(pivot_df.columns[:6]) + cols_ordered]

    return pivot_df

def process_point_trajectory(df_trajectory):

    pt_len = df_trajectory.stroke_idx.max()
    if pt_len > 3:
        # Serve, Return, +1, Last Shot
        return pd.concat([process_stroke_trajectory(df_trajectory[df_trajectory.stroke_idx==i]) for i in np.arange(1,pt_len+1)])
        #return pd.concat([process_stroke_trajectory(df_trajectory[df_trajectory.stroke_idx==i]) for i in [1,2,3,df_trajectory.stroke_idx.unique()[-1]]])
    elif pt_len <= 3:
        return pd.concat([process_stroke_trajectory(df_trajectory[df_trajectory.stroke_idx==i]) for i in np.arange(1,pt_len+1)])
    

# Processing functions for the renamed "match_score" column present in the df_points_sorted returned by process_points_data()
def process_point_score(df_point_r, setend_point_ids, tourn_id):

    df_point_score = df_point_r.match_score
    set_n = df_point_r.set_n

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
    if df_point_r.point_id in setend_point_ids:
        if p1_game_score == "GAME":
            p1_sets_w += 1
        else:
            p2_sets_w += 1

    return {"p1_sets_w": p1_sets_w, "p2_sets_w": p2_sets_w, "p1_set_score": int(p1_set_score), "p2_set_score": int(p2_set_score), \
            "p1_game_score": p1_game_score, "p2_game_score": p2_game_score, "is_tiebreak": is_tiebreak}

# Put all above functions in sequence to process from raw data -> dataframe for atp_court_vision table
def process_court_vision(year, tourn_id, match_id, round_n, raw_data):

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
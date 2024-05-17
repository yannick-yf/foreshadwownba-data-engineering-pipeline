# NbaGames.py

from functions import WebScrapNbaGameData
import pandas as pd

def fn_get_seasons_nba_games_data(SEASON_ARRAY):

    NBA_GAMES_DF = pd.DataFrame()

    for season in SEASON_ARRAY:
        NBA_GAMES_DF_tmp = WebScrapNbaGameData.webscrappe_nba_games_data(season)
        NBA_GAMES_DF = pd.concat([NBA_GAMES_DF, NBA_GAMES_DF_tmp], axis=0)

    return NBA_GAMES_DF

def fn_get_seasons_nba_schedules_overtime_data(SEASON_ARRAY):

    NBA_SCHEDULES_DF = pd.DataFrame()

    for season in SEASON_ARRAY:
        NBA_SCHEDULES_DF_tmp = WebScrapNbaGameData.webscrappe_nba_schedule_overtime_data(season)
        NBA_SCHEDULES_DF = pd.concat([NBA_SCHEDULES_DF, NBA_SCHEDULES_DF_tmp], axis=0)

    return NBA_SCHEDULES_DF

def cleaned_and_combined(GAMES_DF, SCHEDULES_DF):

    #----------------------------------------------
    # SCHEDULES_DF - Re format date
    SCHEDULES_DF['game_date'] = pd.to_datetime(SCHEDULES_DF['game_date'])

    #----------------------------------------------
    # GAMES_DF - Recast date before merging the two dataset
    GAMES_DF['game_date'] = pd.to_datetime(GAMES_DF['game_date'])

    #----------------------------------------------
    # Join the two dataframes
    FinalDF = pd.merge(
        GAMES_DF,
        SCHEDULES_DF[['id_season', 'tm', 'game_date', 'time_start', 'overtime', 'w_tot', 'l_tot', 'streak_w_l']],
        how='left',
        left_on=['id_season', 'tm', 'game_date'],
        right_on=['id_season', 'tm', 'game_date'])

    return FinalDF
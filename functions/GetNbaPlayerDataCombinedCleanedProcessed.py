# NbaGames.py

from functions import WebScrapNbaPlayersGameData
import pandas as pd

def fn_get_nba_list_players_and_attributes_data(SEASON_ARRAY):

    NBA_PLAYERS_GAMES_DF = pd.DataFrame()

    for season in SEASON_ARRAY:
        NBA_PLAYERS_GAMES_DF_tmp = WebScrapNbaPlayersGameData.webscrappe_nba_list_players_and_attributes_data(season)
        NBA_PLAYERS_GAMES_DF = NBA_PLAYERS_GAMES_DF.append(NBA_PLAYERS_GAMES_DF_tmp)

    return NBA_PLAYERS_GAMES_DF

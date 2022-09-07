# NbaGames.py

from functions import WebScrapNbaPlayersGameData, ManualRecodePlayerIdWebscrappe
import pandas as pd
import numpy as np

def fn_get_nba_list_players_and_attributes_data(SEASON_ARRAY):

    NBA_PLAYERS_GAMES_DF = pd.DataFrame()

    for season in SEASON_ARRAY:
        NBA_PLAYERS_GAMES_DF_tmp = WebScrapNbaPlayersGameData.webscrappe_nba_list_players_and_attributes_data(season)
        NBA_PLAYERS_GAMES_DF = NBA_PLAYERS_GAMES_DF.append(NBA_PLAYERS_GAMES_DF_tmp)

    return NBA_PLAYERS_GAMES_DF

# IMPORTANT # https://www.basketball-reference.com/friv/continuity.html
def fn_get_id_webscrappe_for_players_games_data():

    list_players = pd.read_csv('pipeline_output/nba_list_players_multi_season_dataset_2022-08-31.csv')
    list_players = list_players[['id_season', 'tm', 'Name']]

    #-------------------------------
    # Remove ponctuation to the Name 

    list_players['Name'] = list_players['Name'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

    #-------------------------------
    # Remove specifc character 

    list_players['Name'] = list_players['Name'].str.replace("'", '', regex=True)
    list_players['Name'] = list_players['Name'].str.replace("-", '', regex=True)
    list_players['Name'] = list_players['Name'].str.replace(".", '', regex=True)

    #----------------------
    # str split to get name and surnmae

    test = list_players['Name'].str.split(pat = ' ', expand = True)
    list_players[['first_name', 'last_name', 'last_name2', 'last_name3']] = test

    #----------------------
    # Fill na and concat the last names features into one
    list_players = list_players.fillna('')
    list_players['last_name_final'] = list_players['last_name'] + list_players[ 'last_name2'] + list_players[ 'last_name3']

    #----------------------
    # ID webscrappe - part 1

    list_players['id_part_1'] = list_players['last_name_final'].str.lower().str[0]

    #----------------------
    # ID webscrappe - part 2
    # First 5 letters of the surname - if less than 5 character takes all

    list_players["id_part_2"] = np.where(
        list_players["last_name"].str.lower().str.len()<5,
        list_players["last_name"].str.lower(),
        list_players['last_name'].str.lower().str[:5])

    #----------------------
    # ID webscrappe - part 3 -  First two characters of the first name
    list_players["id_part_3"] = list_players['first_name'].str.lower().str[:2]

    #----------------------
    # ID webscrappe - part 4 - by defautl 01 but if duplicate it will be 02
    # the duplicate it handles manaully
    list_players["id_part_4"] = '01'

    #----------------------------------
    # Final id webscrapp creation

    list_players['id_webscrapping'] = list_players['id_part_1'] +\
            '/' +\
            list_players['id_part_2'] +\
            list_players['id_part_3'] +\
            list_players["id_part_4"]

    #----------------------------------
    # Manual Recode due to duplicagtes
    list_players = ManualRecodePlayerIdWebscrappe.manual_recode_id_webscrapping(list_players)

    #----------------------------------------
    # Sort the data frames by names and year
    list_players = list_players.sort_values(by=['Name', 'id_season'])

    return list_players[['id_season', 'Name', 'id_webscrapping']]

# NbaGames.py

from functions import ManualRecodePlayerIdWebscrappe, WebScrapNbaPlayersListAndAttributes, WebScrapNbaPlayersBoxscores, WebScrapNbaSalary
import pandas as pd
import numpy as np
import datetime
import sys

def fn_get_nba_list_players_and_attributes_data(SEASON_ARRAY):

    NBA_PLAYERS_GAMES_DF = pd.DataFrame()

    for season in SEASON_ARRAY:
        NBA_PLAYERS_GAMES_DF_tmp = WebScrapNbaPlayersListAndAttributes.webscrappe_nba_list_players_and_attributes_data(season)
        NBA_PLAYERS_GAMES_DF = NBA_PLAYERS_GAMES_DF.append(NBA_PLAYERS_GAMES_DF_tmp)

    return NBA_PLAYERS_GAMES_DF

#------------------------------------------------------------------------------------------------------------------

def fn_get_players_boxscores_data():

    id_webscrappe_players = pd.read_csv('pipeline_output/nba_id_webscrappe_players_2022-09-07.csv')
    players_boxscores, players_to_add_manually =  WebScrapNbaPlayersBoxscores.webscrappe_nba_players_boxscores_data(id_webscrappe_players)
    players_to_add_manually.to_csv('./pipeline_output/players_to_add_manually_'+ datetime.datetime.today().strftime('%Y-%m-%d') + '.csv' , index = False)

    return players_boxscores

#------------------------------------------------------------------------------------------------------------------

def fn_get_nba_players_salary_data(SEASON_ARRAY):

    NBA_PLAYERS_SALARY_DF = pd.DataFrame()

    for season in SEASON_ARRAY:
        print(season)
        NBA_PLAYERS_SALARY_DF_tmp = WebScrapNbaSalary.GetSalaries(str(season))
        NBA_PLAYERS_SALARY_DF = NBA_PLAYERS_SALARY_DF.append(NBA_PLAYERS_SALARY_DF_tmp)

    return NBA_PLAYERS_SALARY_DF

#------------------------------------------------------------------------------------------------------------------
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

#------------------------------------------------------------------------------------------------------------------
def fn_clean_and_combined_player_attributes_and_salary():

    schedule = pd.read_csv('./pipeline_output/schedules_training_dataset_2022-03-30.csv')
    players_attributes = pd.read_csv('./pipeline_output/nba_list_players_multi_season_dataset_2022-08-31.csv')
    players_salary = pd.read_csv('./pipeline_output/nba_players_salaries_2022-09-12.csv')
    
    # Name cleaning from both player attributes and player salary
    players_attributes['Name'] = players_attributes['Name'].str.replace("'", '', regex=True)
    players_attributes['Name'] = players_attributes['Name'].str.replace("-", '', regex=True)
    players_attributes['Name'] = players_attributes['Name'].str.replace(".", '', regex=True)
    players_attributes['Name'] = players_attributes['Name'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

    players_salary['name'] = players_salary['name'].str.replace("'", '', regex=True)
    players_salary['name'] = players_salary['name'].str.replace("-", '', regex=True)
    players_salary['name'] = players_salary['name'].str.replace(".", '', regex=True)
    players_salary['name'] = players_salary['name'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

    #------------------------
    # Get the Age of player at the start of the season
    # Birth date as date
    players_attributes['BirthDate'] = pd.to_datetime(players_attributes['BirthDate'], format="%B %d, %Y")

    gb = schedule.groupby(['id_season'])
    min_date_season = gb.agg({'game_date' : np.min}).reset_index()

    players_attributes = pd.merge(
        players_attributes,
        min_date_season,
        how='left',
        left_on=['id_season'],
        right_on=['id_season'])
    
    # Age at the start of the season
    players_attributes['Age'] = (pd.to_datetime(players_attributes['game_date']) - pd.to_datetime(players_attributes['BirthDate'])).dt.days
    players_attributes['Age'] = players_attributes['Age'] / 365
    players_attributes['Age'] = pd.to_numeric(players_attributes['Age']).round(2)
    
    #------------------------
    # Get Expereince - Rename Rookie
    players_attributes['Experience'] = pd.to_numeric(np.where(players_attributes['Experience'] == 'R', 0, players_attributes['Experience']  ))

    #------------------------
    # Convert Ht from us sizu to cm size

    players_attributes[['foot','inch']] = players_attributes['Ht'].str.split('-', expand=True)
    players_attributes['cm_size'] = pd.to_numeric(players_attributes['foot']) * 30.48 + pd.to_numeric(players_attributes['inch']) * 2.54

    # Weight to numeric
    players_attributes['Wt'] = pd.to_numeric(players_attributes['Wt'])  

    #------------------------
    # Add Salary
    player_info = pd.merge(
        players_attributes,
        players_salary[['name','year','salary']],
        how='left',
        left_on=['Name', 'id_season'],
        right_on=['name', 'year'])

    #------------------------
    # Get the columns Needed
    player_info = player_info[['id_season', 'tm', 'Name', 'Position', 'Wt', 'Experience', 'Age', 'cm_size', 'salary']]

    return player_info
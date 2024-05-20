
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import requests
import sys
import numpy as np
from urllib.error import HTTPError
import time

#------------------------------------------------------------------------------------------------------------
# Get list of players and attribute per seasons

def webscrappe_nba_list_players_and_attributes_data(SEASON):
    
    #------------------------------------------------------
    # Get team reference data 

    team_city_refdata = pd.read_csv('./constants/team_city_refdata.csv', sep = ';')

    #------------------------------------------------------
    # Initialization of the dataframe to fill-in
    players = pd.DataFrame()

    #------------------------------------------------------
    # For Loop Throught all the team abrev for the given season

    for index, row in team_city_refdata.iterrows():
        # URL to scrape
        team = row['team_abrev']
        url = f"https://www.basketball-reference.com/teams/{team}/{SEASON}.html"
        # url = f"https://www.basketball-reference.com/teams/BRK/2001.html"
        print(url)

        # collect HTML data and create beautiful soup object:
        # collect HTML data
        try:
            html = urlopen(url)
            
            # create beautiful soup object from HTML
            soup = BeautifulSoup(html, "html.parser" )

            # rows = soup.findAll('tr')[2:]
            rows = soup.findAll('table')[0]
            rows = rows.find_all('tr')

            rows_data = [[td.getText() for td in rows[i].findAll('td')]
                                for i in range(len(rows))]

            if len(rows_data) != 0:
                # create the dataframe
                players_tmp = pd.DataFrame(rows_data)

                cols = [
                    'Name', 
                    'Position', 
                    'Ht', 
                    'Wt', 
                    'BirthDate',
                    'Nationality',
                    'Experience', 
                    'College']

                players_tmp.columns =  cols
                players_tmp = players_tmp.dropna()
                players_tmp['id_season'] = SEASON
                players_tmp['tm'] = team

                players = pd.concat([players, players_tmp], axis=0)

        except HTTPError as http_error:
            continue

        time.sleep(5)

    players = players[[
        'id_season', 
        'tm',
        'Name', 
        'Position', 
        'Ht', 
        'Wt', 
        'BirthDate',
        'Nationality',
        'Experience', 
        'College']]

    return players

#------------------------------------------------------------------------------------------------------------
# Get boxscores fo all the players in the flat files saved

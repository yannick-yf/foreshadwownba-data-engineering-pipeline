
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import requests
import sys
import numpy as np
from urllib.error import HTTPError


#------------------------------------------------------------------------------------------------------------
# Get boxscores fo all the players in the flat files saved

def webscrappe_nba_players_boxscores_data(nba_id_webscrappe_players):
    
    #------------------------------------------------------
    # Initialisaton of dataframe we will populated
    
    players = pd.DataFrame()
    players_to_add_manually = pd.DataFrame()

    #------------------------------------------------------
    # For Loop Throught all the team abrev for the given season

    for index, row in nba_id_webscrappe_players.iterrows():
        # URL to scrape
        player_id = row['id_webscrapping']
        player_name = row['Name']
        id_season = str(row['id_season'])
        url = f'https://www.basketball-reference.com/players/{player_id}/gamelog/{id_season}'

        # collect HTML data and create beautiful soup object:
        # collect HTML data
        try:
            html = urlopen(url)
            
            # create beautiful soup object from HTML
            soup = BeautifulSoup(html, "html.parser" )

            try:
                rows = soup.findAll('table')[-1]
                rows = rows.find_all('tr')

                rows_data = [[td.getText() for td in rows[i].findAll('td')]
                                    for i in range(len(rows))]

                if len(rows_data) != 0:
                    # create the dataframe
                    players_tmp = pd.DataFrame(rows_data)
                    players_tmp = players_tmp.reset_index()

                    cols = [
                        'rk', 'game_id', 'game_date',
                        'age',
                        'tm','ext_dom', 'ppp','results',
                        'gs', 'mp',
                        'fg', 'fga', 'fg_prct%', '3p', '3pa', '3p_prct', 'ft',	'fta',	'ft_prct', 
                        'orb',	'drb',	'trb',	
                        'ast',	
                        'stl',	'blk',	'tov',	'pf',	
                        'pts',	'player_game_score', 'plus_minus']

                    players_tmp.columns =  cols
                    players_tmp = players_tmp.dropna()
                    players_tmp['id_season'] = id_season
                    players_tmp['player_name'] = player_name

                    players = players.append(players_tmp)

            except:
                players_to_add_manually = players_to_add_manually.append(row)
                print("An exception occurred")

        except HTTPError as http_error:
            # print(row)
            players_to_add_manually = players_to_add_manually.append(row)
            continue

    players = players[[
        'id_season','player_name',
        'rk', 'game_id', 'game_date',
        'age',
        'tm','ext_dom', 'ppp','results',
        'gs', 'mp',
        'fg', 'fga', 'fg_prct%', '3p', '3pa', '3p_prct', 'ft',	'fta',	'ft_prct', 
        'orb',	'drb',	'trb',	
        'ast',	
        'stl',	'blk',	'tov',	'pf',	
        'pts',	'player_game_score', 'plus_minus']]

    return players, players_to_add_manually





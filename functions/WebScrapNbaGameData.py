
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import requests
import time

def webscrappe_nba_games_data(SEASON):
    
    #------------------------------------------------------
    # Get team reference data 

    team_city_refdata = pd.read_csv('./constants/team_city_refdata.csv', sep = ';')

    #------------------------------------------------------
    # Initialization of the dataframe to fill-in
    games = pd.DataFrame()

    #------------------------------------------------------
    # For Loop Throught all the team abrev for the given season

    for index, row in team_city_refdata.iterrows():
        # URL to scrape
        team = row['team_abrev']
        url = f"https://www.basketball-reference.com/teams/{team}/{SEASON}/gamelog/"

        if str(requests.get(url)) == '<Response [202]>':

            # collect HTML data and create beautiful soup object:
            # collect HTML data
            html = urlopen(url)
                    
            # create beautiful soup object from HTML
            soup = BeautifulSoup(html, "html.parser" )

            rows = soup.findAll('tr')[2:]

            rows_data = [[td.getText() for td in rows[i].findAll('td')]
                                for i in range(len(rows))]

            if len(rows_data) != 0:
                # create the dataframe
                games_tmp = pd.DataFrame(rows_data)
                cols = ["game_nb", "game_date", "extdom", "opp", "results",
                        "pts_tm","pts_opp",
                        "fg_tm", "fga_tm","fg_prct_tm",
                        "3p_tm","3pa_tm", "3p_prct_tm","ft_tm","fta_tm","ft_prct_tm",
                        "orb_tm","trb_tm", "ast_tm","stl_tm","blk_tm" ,"tov_tm","pf_tm",
                        "nc",
                        "fg_opp","fga_opp","fg_prct_opp",
                        "3p_opp", "3pa_opp", "3p_prct_opp", "ft_opp", "fta_opp","ft_prct_opp",
                        "orb_opp", "trb_opp","ast_opp", "stl_opp", "blk_opp","tov_opp", "pf_opp"]

                games_tmp.columns =  cols
                games_tmp = games_tmp.dropna()
                games_tmp['id_season'] = SEASON
                games_tmp['tm'] = team

                games = pd.concat([games, games_tmp], axis=0)
        
        time.sleep(10)

    games = games[[
        'id_season', 'game_nb', 'game_date', 'extdom', 'tm','opp', 'results', 'pts_tm', 'pts_opp',
        'fg_tm', 'fga_tm', 'fg_prct_tm', '3p_tm', '3pa_tm', '3p_prct_tm',
        'ft_tm', 'fta_tm', 'ft_prct_tm', 'orb_tm', 'trb_tm', 'ast_tm', 'stl_tm',
        'blk_tm', 'tov_tm', 'pf_tm', 'fg_opp', 'fga_opp', 'fg_prct_opp',
        '3p_opp', '3pa_opp', '3p_prct_opp', 'ft_opp', 'fta_opp', 'ft_prct_opp',
        'orb_opp', 'trb_opp', 'ast_opp', 'stl_opp', 'blk_opp', 'tov_opp',
        'pf_opp']]

    return games

#------------------------------------------------------------------------------------------------------------

def webscrappe_nba_schedule_overtime_data(SEASON):
    
    #------------------------------------------------------
    # Get team reference data 

    team_city_refdata = pd.read_csv('./constants/team_city_refdata.csv', sep = ';')

    #------------------------------------------------------
    # Initialization of the dataframe to fill-in
    schedules = pd.DataFrame()

    #------------------------------------------------------
    # For Loop Throught all the team abrev for the given season

    for index, row in team_city_refdata.iterrows():
        
        # URL to scrape
        team = row['team_abrev']

        # URL to scrape
        url = f"https://www.basketball-reference.com/teams/{team}/{SEASON}_games.html"

        if str(requests.get(url)) == '<Response [202]>':

            # collect HTML data and create beautiful soup object:
            # collect HTML data
            html = urlopen(url)
                    
            # create beautiful soup object from HTML
            soup = BeautifulSoup(html,  "html.parser")

            # use getText()to extract the headers into a list
            titles = [th.getText() for th in soup.findAll('tr', limit=2)[1].findAll('th')]

            rows = soup.findAll('tr')[1:]

            rows_data = [[td.getText() for td in rows[i].findAll('td')]
                            for i in range(len(rows))]

            if len(rows_data) != 0:
                # create the dataframe
                schedule_tmp = pd.DataFrame(rows_data)

                cols = ['game_date', 'time_start', 'nc1', 'nc2', 'extdom', 'opponent', 'w_l', 'overtime', 'pts_tm', 'pts_opp', 'w_tot', 'l_tot', 'streak_w_l', 'nc3']
                schedule_tmp.columns =  cols

                schedule_tmp = schedule_tmp.dropna()

                schedule_tmp['id_season'] = SEASON
                schedule_tmp['tm'] = team

                schedules = pd.concat([schedules, schedule_tmp], axis=0)

        time.sleep(10)

    schedules = schedules[['id_season', 'tm', 'game_date', 'time_start', 'extdom', 'opponent', 'w_l', 'overtime', 'pts_tm', 'pts_opp', 'w_tot', 'l_tot', 'streak_w_l']]
            
    return schedules
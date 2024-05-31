import pandas as pd
import numpy as np
from typing import Text
import yaml
import argparse
import os
import glob
import sys
from src.utils.logs import get_logger

def player_attributes_salaries_unification(config_path: Text) -> pd.DataFrame:
    """Load raw data.
    Args:
        config_path {Text}: path to config
    """
    with open("params.yaml") as conf_file:
        config = yaml.safe_load(conf_file)

    logger = get_logger(
        'PLAYER_ATTRIBUTES_SALARIES_UNIFICATION', 
        log_level=config['base']['log_level'])
    
    #------------------------------------------
    # Read the data

    schedule_df = pd.concat(map(pd.read_csv, glob.glob(os.path.join('pipeline_output/schedule/', "schedule_*.csv"))))
    players_attributes = pd.concat(map(pd.read_csv, glob.glob(os.path.join('pipeline_output/player_attributes/', "player_attributes_*.csv"))))
    players_salary = pd.concat(map(pd.read_csv, glob.glob(os.path.join('pipeline_output/player_salary/', "player_salary_*.csv"))))
    
    # Name cleaning from both player attributes and player salary
    players_attributes['Name'] = players_attributes['Name'].str.replace("'", '', regex=True)
    players_attributes['Name'] = players_attributes['Name'].str.replace("-", '', regex=True)
    players_attributes['Name'] = players_attributes['Name'].str.replace(".", '', regex=False)
    players_attributes['Name'] = players_attributes['Name'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

    players_salary['name'] = players_salary['name'].str.replace("'", '', regex=True)
    players_salary['name'] = players_salary['name'].str.replace("-", '', regex=True)
    players_salary['name'] = players_salary['name'].str.replace(".", '', regex=False)
    players_salary['name'] = players_salary['name'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

    #------------------------
    # Get the Age of player at the start of the season
    # Birth date as date
    players_attributes['BirthDate'] = pd.to_datetime(players_attributes['BirthDate'], format="%B %d, %Y")

    gb = schedule_df.groupby(['id_season'])
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

    #------------------------
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

    #------------------------------------------
    # Saving final training dataset

    data_path = config['player_attributes_salaries_unification']['data_path']
    file_name = config['player_attributes_salaries_unification']['file_name']
    
    isExist = os.path.exists(data_path)
    if not isExist:
        os.makedirs(data_path)

    name_and_path_file = data_path + '/' +\
        file_name + '.csv'
    
    player_info.to_csv(
        name_and_path_file, 
        index=True
        )
    
    logger.info('Player Atributes & Salaries Unification complete')

if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument(
        '--config', 
        dest='config', 
        required=True)
    
    args = arg_parser.parse_args()

    player_attributes_salaries_unification(
        config_path = args.config
        )


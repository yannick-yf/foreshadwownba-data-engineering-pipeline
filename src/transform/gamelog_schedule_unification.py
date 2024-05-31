import pandas as pd
from typing import Text
import yaml
import argparse
import os
import glob

from src.utils.logs import get_logger

def gamelog_schedule_unification(config_path: Text) -> pd.DataFrame:
    """Load raw data.
    Args:
        config_path {Text}: path to config
    """
    with open("params.yaml") as conf_file:
        config = yaml.safe_load(conf_file)

    logger = get_logger(
        'GAMELOG_SCHEDULE_UNIFICATION', 
        log_level=config['base']['log_level'])
    
    #------------------------------------------
    # Read the data

    gamelog_df = pd.concat(map(pd.read_csv, glob.glob(os.path.join('pipeline_output/gamelog/', "gamelog_*.csv"))))
    schedule_df = pd.concat(map(pd.read_csv, glob.glob(os.path.join('pipeline_output/schedule/', "schedule_*.csv"))))

    #----------------------------------------------
    # SCHEDULES_DF - Re format date
    schedule_df['game_date'] = pd.to_datetime(schedule_df['game_date'])

    #----------------------------------------------
    # GAMES_DF - Recast date before merging the two dataset
    gamelog_df['game_date'] = pd.to_datetime(gamelog_df['game_date'])

    #----------------------------------------------
    # Join the two dataframes
    nba_games_training_dataset = pd.merge(
        gamelog_df,
        schedule_df[['id_season', 'tm', 'game_date', 'time_start', 'overtime', 'w_tot', 'l_tot', 'streak_w_l']],
        how='left',
        left_on=['id_season', 'tm', 'game_date'],
        right_on=['id_season', 'tm', 'game_date'])

    #------------------------------------------
    # Saving final training dataset

    data_path = config['gamelog_schedule_unification']['data_path']
    file_name = config['gamelog_schedule_unification']['file_name']
    
    isExist = os.path.exists(data_path)
    if not isExist:
        os.makedirs(data_path)

    name_and_path_file = data_path + '/' +\
        file_name + '.csv'
    
    nba_games_training_dataset.to_csv(
        name_and_path_file, 
        index=True
        )
    
    logger.info('Gamelog & Schedule Unification complete')

if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument(
        '--config', 
        dest='config', 
        required=True)
    
    args = arg_parser.parse_args()

    gamelog_schedule_unification(
        config_path = args.config
        )


import pandas as pd
from typing import Text
import yaml
import argparse
import os

from basketball_reference_webscrapper.webscrapping_basketball_reference import (
    WebScrapBasketballReference,
)
from basketball_reference_webscrapper.data_models.feature_model import FeatureIn

from src.utils.logs import get_logger

def player_attributes_data_acquisition(config_path: Text) -> pd.DataFrame:
    """Load raw data.
    Args:
        config_path {Text}: path to config
    """
    with open("params.yaml") as conf_file:
        config = yaml.safe_load(conf_file)

    logger = get_logger(
        'PLAYER_ATTRIBUTES_DATA_ACQUISITION', 
        log_level=config['base']['log_level'])
    
    player_attributes_df = WebScrapBasketballReference(
        FeatureIn(
            data_type=config['player_attributes_data_acquisition']['data_type'], 
            season=config['player_attributes_data_acquisition']['season'],
            team=config['player_attributes_data_acquisition']['team'])
    ).webscrappe_nba_games_data()

    #------------------------------------------
    # Saving final training dataset

    season = config['player_attributes_data_acquisition']['season']
    team = config['player_attributes_data_acquisition']['team']
    folder = config['player_attributes_data_acquisition']['output_folder']

    isExist = os.path.exists(folder)
    if not isExist:
        os.makedirs(folder)

    name_and_path_file = folder + '/' +\
        config['player_attributes_data_acquisition']['data_type'] +\
        '_' +\
        str(season) +\
        '_' +\
        team + '.csv'
    
    player_attributes_df.to_csv(
        name_and_path_file, 
        index=True
        )
    
    logger.info('Player Attributes Data Acquisition complete')

if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument(
        '--config', 
        dest='config', 
        required=True)
    
    args = arg_parser.parse_args()

    player_attributes_data_acquisition(
        config_path = args.config
        )


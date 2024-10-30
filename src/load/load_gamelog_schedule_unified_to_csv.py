# https://sebrave.medium.com/how-to-spin-up-a-local-mysql-database-on-macos-a550918f092b
# https://blog.devart.com/delete-duplicate-rows-in-mysql.html

import pymysql
import pandas as pd
from typing import Text
import yaml
import argparse
import os
from pandas.io import sql
from sqlalchemy import create_engine
from src.utils.logs import get_logger
import sys
from sqlalchemy import text
# importing os module for environment variables
import os
# importing necessary functions from dotenv library
from dotenv import load_dotenv 

def load_gamelog_schedule_unified_to_csv(config_path: Text) -> pd.DataFrame:
    """Load raw data.
    Args:
        config_path {Text}: path to config
    """
    with open("params.yaml") as conf_file:
        config_params = yaml.safe_load(conf_file)

    # loading variables from .env file
    load_dotenv() 

    logger = get_logger(
        "LOAD_DATA_TO_DATABASE", log_level=config_params["base"]["log_level"]
    )

    # Read the data to insert into the db
    gamelog_schedule_input_path = config_params["gamelog_schedule_unification"]["data_path"]
    gamelog_schedule_input_name = config_params["gamelog_schedule_unification"]["file_name"]

    gamelog_schedule_output_path = config_params["load_gamelog_schedule_unified_to_csv"]["data_path"]
    gamelog_schedule_output_name = config_params["load_gamelog_schedule_unified_to_csv"]["file_name"]

    input_name_and_path_file = str(gamelog_schedule_input_path) + '/' + gamelog_schedule_input_name + ".csv"
    output_name_and_path_file = str(gamelog_schedule_output_path) + '/' + gamelog_schedule_output_name + ".csv"

    nba_games_training_dataset = pd.read_csv(input_name_and_path_file)
    nba_games_training_dataset = nba_games_training_dataset.reset_index(drop=True)

    # Name of the flat files
    # nba_gamelog_schedule_dataset
    isExist = os.path.exists(gamelog_schedule_output_path)
    if not isExist:
        os.makedirs(gamelog_schedule_output_path)

    nba_games_training_dataset.to_csv(output_name_and_path_file, index=False)

    # NExt step will be to save it in S3
    # TODO Save to s3 bucket

    logger.info("Load Gamelog and Schedule Data to Database complete")


if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument("--config-params", dest="config_params", required=True)

    args = arg_parser.parse_args()

    load_gamelog_schedule_unified_to_csv(config_path=args.config_params)

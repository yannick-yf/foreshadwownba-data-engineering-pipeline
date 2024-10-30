# https://sebrave.medium.com/how-to-spin-up-a-local-mysql-database-on-macos-a550918f092b
# https://blog.devart.com/delete-duplicate-rows-in-mysql.html
# https://numberly.tech/orchestrating-python-workflows-in-apache-airflow-fd8be71ad504

import pymysql
import pandas as pd
from typing import Text
import yaml
import argparse
import os
from pandas.io import sql
from sqlalchemy import create_engine
from src.utils.logs import get_logger
from sqlalchemy import text
import os
from dotenv import load_dotenv 

def load_player_attributes_salaries_unified_to_csv(config_path: Text) -> pd.DataFrame:
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
    player_attributes_salaries_path = config_params["player_attributes_salaries_unification"]["data_path"]
    player_attributes_salaries_name = config_params["player_attributes_salaries_unification"]["file_name"]

    name_and_path_file = str(player_attributes_salaries_path) + '/' + player_attributes_salaries_name + ".csv"

    player_attributes_salaries_dataset = pd.read_csv(name_and_path_file)
    player_attributes_salaries_dataset = player_attributes_salaries_dataset.reset_index(drop=True)

    player_attributes_salaries_dataset.to_csv(name_and_path_file, index=False)

    # NExt step will be to save it in S3
    # TODO Save to s3 bucket

    logger.info("Load Player and Attributes Data to Database complete")

if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument("--config-params", dest="config_params", required=True)

    args = arg_parser.parse_args()

    load_player_attributes_salaries_unified_to_csv(config_path=args.config_params)

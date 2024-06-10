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
# importing os module for environment variables
import os
# importing necessary functions from dotenv library
from dotenv import load_dotenv 

def load_data_to_db(config_path: Text) -> pd.DataFrame:
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
    nba_games_training_dataset = pd.read_csv('pipeline_output/final/nba_games_training_dataset.csv')
    nba_games_training_dataset = nba_games_training_dataset.reset_index(drop=True)

    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                        .format(user=os.getenv('MYSQL_USERNAME'),
                                pw=os.getenv("MYSQL_PASSWORD"),
                                db=os.getenv("MYSQL_DATABASE"),))
    
    nba_games_training_dataset.to_sql(
        con=engine, 
        index=False,
        name='nba_games_training_dataset', 
        if_exists='replace')

# DELETE
#   FROM customer_movie_rentals
# WHERE rental_id IN (SELECT
#       rental_id
#     FROM (SELECT
#         rental_id,
#         ROW_NUMBER() OVER (
#         PARTITION BY customer_id
#         ORDER BY customer_id
#         ) AS row_num
#       FROM customer_movie_rentals cmr) t
#     WHERE row_num > 1);

    logger.info("Load Data to Database complete")


if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument("--config-params", dest="config_params", required=True)

    args = arg_parser.parse_args()

    load_data_to_db(config_path=args.config_params)

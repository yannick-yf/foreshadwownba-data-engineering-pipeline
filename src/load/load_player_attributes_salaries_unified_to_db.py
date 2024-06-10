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
from sqlalchemy import text
import os
from dotenv import load_dotenv 

def load_player_attributes_salaries_unified_to_db(config_path: Text) -> pd.DataFrame:
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

    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                        .format(user=os.getenv('MYSQL_USERNAME'),
                                pw=os.getenv("MYSQL_PASSWORD"),
                                db=os.getenv("MYSQL_DATABASE")))
    
    player_attributes_salaries_dataset.to_sql(
        con=engine, 
        index=False,
        name=player_attributes_salaries_name, 
        if_exists='append')
    
    with engine.connect() as conn:
        query1 = text("""
            ALTER TABLE player_attributes_salaries_dataset ADD COLUMN count_ID int(10) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY;
        """)
        query2 = text("""
            DELETE FROM player_attributes_salaries_dataset
            WHERE count_ID not in (
                SELECT count_ID FROM (
                    SELECT max(count_ID) as count_ID
                    FROM player_attributes_salaries_dataset
                    GROUP BY id_season, tm, Name, Position
                    ) as c
                );
        """)
        query3 = text("""
            ALTER TABLE player_attributes_salaries_dataset DROP count_ID;
        """)
        conn.execute(query1)
        conn.execute(query2)
        conn.execute(query3)

    logger.info("Load Player and Attributes Data to Database complete")


if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument("--config-params", dest="config_params", required=True)

    args = arg_parser.parse_args()

    load_player_attributes_salaries_unified_to_db(config_path=args.config_params)

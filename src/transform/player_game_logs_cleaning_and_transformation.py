import pandas as pd
import numpy as np
from typing import Text
import yaml
import argparse
import os
import glob
from typing import Union
from pathlib import Path
import sys
from src.utils.logs import get_logger

logger = get_logger(
    "PLAYER_GAME_LOGS_CLEANING_AND_TRANSFORMATION", log_level='INFO'
)

def player_game_logs_cleaning_and_transformation(
        file_name: str = 'player_gamelog_2023_all',
        input_folder: Path = 'pipeline_output/player_game_logs/',
        output_folder: Path = 'pipeline_output/player_game_logs_cleaned/'
        ) -> None:
    """
    Cleaning and transformation of player game logs dataframe.

    Args:
        file_name (str): Name of the file to process (without .csv extension)
        input_folder (Path): Path where raw player game logs data is stored
        output_folder (Path): Path where to save the cleaned player game logs data
    """

    # ------------------------------------------
    # Read the data
    file_path_name = str(input_folder) + '/' + str(file_name) + ".csv"
    player_game_logs_df = pd.read_csv(file_path_name)
    player_game_logs_df = player_game_logs_df.reset_index(drop=True)

    # ----------------------------------------------
    # PLAYER_GAME_LOGS_DF - Recast date before further processing
    if 'game_date' in player_game_logs_df.columns:
        player_game_logs_df["game_date"] = pd.to_datetime(player_game_logs_df["game_date"])

    #-------------------------------------------
    # Check for Duplicate rows and handle them
    # Assuming player game logs have player_id, team, and game_date as key identifiers
    duplicate_columns = []
    if 'player_id' in player_game_logs_df.columns:
        duplicate_columns.append('player_id')
    if 'player' in player_game_logs_df.columns:
        duplicate_columns.append('player')
    if 'tm' in player_game_logs_df.columns:
        duplicate_columns.append('tm')
    if 'game_date' in player_game_logs_df.columns:
        duplicate_columns.append('game_date')

    if duplicate_columns:
        nb_duplicated_rows = player_game_logs_df.duplicated(subset=duplicate_columns, keep='first').sum()

        if nb_duplicated_rows > 0:
            logger.info(f'DUPLICATED ROWS IN THE DATAFRAME: {nb_duplicated_rows}')
            player_game_logs_df = player_game_logs_df.drop_duplicates(subset=duplicate_columns)
        else:
            logger.info('No duplicated rows')

    #-------------------------------------------
    # Format game_date to string format
    if 'game_date' in player_game_logs_df.columns:
        player_game_logs_df['game_date'] = player_game_logs_df['game_date'].astype(str).str[:10]

    #-------------------------------------------
    # Create unique id for each player game log entry
    if 'player_id' in player_game_logs_df.columns and 'game_date' in player_game_logs_df.columns:
        player_game_logs_df['id'] = player_game_logs_df['player_id'].astype(str) + '_' + player_game_logs_df['game_date']
    elif 'player' in player_game_logs_df.columns and 'game_date' in player_game_logs_df.columns and 'tm' in player_game_logs_df.columns:
        player_game_logs_df['id'] = player_game_logs_df['player'].astype(str) + '_' + player_game_logs_df['tm'].astype(str) + '_' + player_game_logs_df['game_date']

    # ------------------------------------------
    # Saving final cleaned dataset

    isExist = os.path.exists(output_folder)
    if not isExist:
        os.makedirs(output_folder)

    name_and_path_file = str(output_folder) + '/' + file_name + ".csv"

    player_game_logs_df.to_csv(name_and_path_file, index=False)

    logger.info("Player Game Logs cleaning and transformation complete")

def get_args():
    """
    Parse command line arguments and return the parsed arguments.

    Returns:
        argparse.Namespace: Parsed command line arguments.
    """
    _dir = Path(__file__).parent.resolve()
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--params-file",
        type=Path,
        default="params.yaml",
    )

    args, _ = parser.parse_known_args()
    params = yaml.safe_load(args.params_file.open())

    player_game_logs_data_acquisition = params["player_game_logs_data_acquisition"]
    player_game_logs_cleaning_and_transformation_params = params["player_game_logs_cleaning_and_transformation"]
    global_params = params['global_params']

    parser.add_argument(
        "--input-folder",
        dest="input_folder",
        type=Path,
        default=player_game_logs_data_acquisition["output_folder"],
    )

    parser.add_argument(
        "--output-folder",
        dest="output_folder",
        type=Path,
        default=player_game_logs_cleaning_and_transformation_params["output_folder"],
    )

    global_params["season"]

    file_name = player_game_logs_data_acquisition["data_type"] +\
        "_" +\
        str(global_params["season"]) +\
        "_" +\
        player_game_logs_data_acquisition["team"]

    parser.add_argument(
        "--file-name",
        dest="file_name",
        type=str,
        default=file_name,
    )

    args = parser.parse_args()

    args.output_folder.parent.mkdir(
        parents=True,
        exist_ok=True)

    return args

def main():
    """Run the Player Game Logs Cleaning and Transformation Pipeline."""
    args = get_args()

    player_game_logs_cleaning_and_transformation(
        file_name=args.file_name,
        input_folder=args.input_folder,
        output_folder=args.output_folder
    )

if __name__ == "__main__":
    main()

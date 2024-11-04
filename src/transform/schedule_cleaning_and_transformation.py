import pandas as pd
import numpy as np
from typing import Text
import yaml
import argparse
import os
from pathlib import Path

from src.utils.logs import get_logger

logger = get_logger(
    "SCHEDULE_CLEANING_AND_TRANSFORMATION", log_level='INFO'
)

def schedule_cleaning_and_transformation(
        file_name: str = 'schedule_2023_all',
        input_folder: Path = 'pipeline_output/schedule/',
        output_folder: Path = 'pipeline_output/schedule_cleaned/'
        ) -> None:
    """
    Unification of gamelogs and schdules dataframes.

    Args:
        data_type (str): Argument from basketball_reference_webscrapper. Type of data to pull from the package
        season (int): Argument from basketball_reference_webscrapper. Season to pull from the package
        team: Path (str): Argument from basketball_reference_webscrapper. Default is 'all'. Team to pull data from the package.
        output_folder (Path): Path where to save the gamelog data pulled using the package.
    """

    # ------------------------------------------
    # Read the data
    file_path_name = str(input_folder) + '/' +str(file_name) +  ".csv"
    schedule_df = pd.read_csv(file_path_name)
    schedule_df = schedule_df.reset_index(drop=True)

    # ----------------------------------------------
    # GAMES_DF - Recast date before merging the two dataset
    schedule_df["game_date"] = pd.to_datetime(schedule_df["game_date"])

    #-------------------------------------------
    # Check for Duplciate and raise errors
    nb_duplicated_rows = schedule_df.duplicated(subset=["id_season", "tm", "game_date"], keep='first').sum()

    if nb_duplicated_rows > 0:
        logger.info('DUPLICATED ROWS IN THE DATAFRAME')
    else:
        logger.info('No duplicated rows')

    schedule_df = schedule_df.drop_duplicates(subset=["id_season", "tm", "game_date"])

    #-------------------------------------------
    # Ext Dom Process

    schedule_df['extdom'] = np.where(
        schedule_df['extdom']=='@',
        'ext',
        'dom')
    
    schedule_df['game_date'] = schedule_df['game_date'].astype(str).str[:10]

    # ------------------------------------------
    # Saving final training dataset

    isExist = os.path.exists(output_folder)
    if not isExist:
        os.makedirs(output_folder)

    name_and_path_file = str(output_folder)+ '/' + file_name + ".csv"

    schedule_df.to_csv(name_and_path_file, index=False)

    logger.info("Schedule cleaning and transformation complete")

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

    schedule_data_acquisition = params["schedule_data_acquisition"]
    schedule_cleaning_and_transformation_params = params["schedule_cleaning_and_transformation"]
    global_params = params['global_params']

    parser.add_argument(
        "--input-folder",
        dest="input_folder",
        type=Path,
        default=schedule_data_acquisition["output_folder"],
    )

    parser.add_argument(
        "--output-folder",
        dest="output_folder",
        type=Path,
        default=schedule_cleaning_and_transformation_params["output_folder"],
    )

    global_params["season"]

    file_name = schedule_data_acquisition["data_type"] +\
        "_" +\
        str(global_params["season"]) +\
        "_" +\
        schedule_data_acquisition["team"]
    
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
    """Run the Pre Train Multiple Models Pipeline."""
    args = get_args()

    schedule_cleaning_and_transformation(
        file_name=args.file_name,
        input_folder=args.input_folder,
        output_folder=args.output_folder
    )

if __name__ == "__main__":
    main()

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
    "GAMELOG_SCHEDULE_UNIFICATION", log_level='INFO'
)

def gamelog_schedule_unification(
        gamelog_data_path: Path ='pipeline_output/gamelog/',
        gamelog_name_pattern: str = 'gamelog',
        schedule_data_path: Path ='pipeline_output/schedule/',
        schedule_name_pattern: str = 'schedule',
        unified_file_path: Path ='pipeline_output/final/',
        unified_file_name: str = 'nba_games_training_dataset'
        ) -> None:
    """
    Unification of gamelogs and schdules dataframes.

    Args:
        gamelog_data_path: Path where to read the gamelog dataframe.
        gamelog_name_pattern (str): gamelog name pattern to read mutliple season gamelog
        schedule_data_path: Path where to read the schedule dataframe.
        schedule_name_pattern (str): schedule name pattern to read mutliple season schedule
        unified_file_path (Path): Path where to save the unified dataframe.
        unified_file_name (str): Name of the unfied dataframe
    """

    # ------------------------------------------
    # Read the data

    gamelog_df = pd.concat(
        map(
            pd.read_csv,
            glob.glob(os.path.join(gamelog_data_path, gamelog_name_pattern +  "_*.csv")),
        )
    )
    gamelog_df = gamelog_df.reset_index(drop=True)

    schedule_df = pd.concat(
        map(
            pd.read_csv,
            glob.glob(os.path.join(schedule_data_path, schedule_name_pattern +  "_*.csv")),
        )
    )
    schedule_df = schedule_df.reset_index(drop=True)

    # ----------------------------------------------
    # SCHEDULES_DF - Re format date
    schedule_df["game_date"] = pd.to_datetime(schedule_df["game_date"])

    # ----------------------------------------------
    # GAMES_DF - Recast date before merging the two dataset
    gamelog_df["game_date"] = pd.to_datetime(gamelog_df["game_date"])

    # ----------------------------------------------
    # Join the two dataframes
    nba_games_training_dataset = pd.merge(
        gamelog_df,
        schedule_df[
            [
                "id_season",
                "tm",
                "game_date",
                "time_start",
                "overtime",
                "w_tot",
                "l_tot",
                "streak_w_l",
            ]
        ],
        how="left",
        left_on=["id_season", "tm", "game_date"],
        right_on=["id_season", "tm", "game_date"],
    )

    #-------------------------------------------
    # Check for Duplciate and raise errors
    nb_duplicated_rows = nba_games_training_dataset.duplicated(subset=["id_season", "tm", "game_date"], keep='first').sum()

    if nb_duplicated_rows > 0:
        logger.info('DUPLICATED ROWS IN THE DATAFRAME')

    nba_games_training_dataset = nba_games_training_dataset.drop_duplicates(subset=["id_season", "tm", "game_date"])

    #-------------------------------------------
    # Ext Dom Process

    nba_games_training_dataset['extdom'] = np.where(
        nba_games_training_dataset['extdom']=='@',
        'ext',
        'dom')
    
    nba_games_training_dataset['game_date'] = nba_games_training_dataset['game_date'].astype(str).str[:10]

    #-------------------------------------------
    # Unique id creation
    nba_games_training_dataset['id'] = np.where(
        nba_games_training_dataset['extdom']=='dom',
        nba_games_training_dataset['game_date'] + '_' + nba_games_training_dataset['opp'] + '_' + nba_games_training_dataset['tm'],
        nba_games_training_dataset['game_date'] + '_' + nba_games_training_dataset['tm'] + '_' + nba_games_training_dataset['opp']
    )

    # ------------------------------------------
    # Saving final training dataset

    isExist = os.path.exists(unified_file_path)
    if not isExist:
        os.makedirs(unified_file_path)

    name_and_path_file = str(unified_file_path)+ '/' + unified_file_name + ".csv"

    nba_games_training_dataset.to_csv(name_and_path_file, index=False)

    logger.info("Gamelog & Schedule Unification complete")

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

    gamelog_schedule_unification_params = params["gamelog_schedule_unification"]

    parser.add_argument(
        "--unified-file-path",
        dest="unified_file_path",
        type=Path,
        default=gamelog_schedule_unification_params["data_path"],
    )

    parser.add_argument(
        "--unified-file-name",
        dest="unified_file_name",
        type=str,
        default=gamelog_schedule_unification_params["file_name"],
    )

    gamelog_data_acquisition = params["gamelog_data_acquisition"]

    parser.add_argument(
        "--gamelog-data-path",
        dest="gamelog_data_path",
        type=Path,
        default=gamelog_data_acquisition["output_folder"],
    )

    parser.add_argument(
        "--gamelog-name-pattern",
        dest="gamelog_name_pattern",
        type=str,
        default=gamelog_data_acquisition["data_type"],
    )

    schedule_data_acquisition = params["schedule_data_acquisition"]

    parser.add_argument(
        "--schedule-data-path",
        dest="schedule_data_path",
        type=Path,
        default=schedule_data_acquisition["output_folder"],
    )

    parser.add_argument(
        "--schedule-name-pattern",
        dest="schedule_name_pattern",
        type=str,
        default=schedule_data_acquisition["data_type"],
    )

    args = parser.parse_args()

    args.unified_file_path.parent.mkdir(
        parents=True, 
        exist_ok=True)

    return args

def main():
    """Run the Pre Train Multiple Models Pipeline."""
    args = get_args()

    gamelog_schedule_unification(
        gamelog_data_path=args.gamelog_data_path,
        gamelog_name_pattern=args.gamelog_name_pattern,
        schedule_data_path=args.schedule_data_path,
        schedule_name_pattern=args.schedule_name_pattern,
        unified_file_path=args.unified_file_path,
        unified_file_name=args.unified_file_name,
    )

if __name__ == "__main__":
    main()

import pandas as pd
from typing import Text
import yaml
import argparse
import os
import glob
from typing import Union
from pathlib import Path

from src.utils.logs import get_logger

logger = get_logger(
    "GAMELOG_SCHEDULE_UNIFICATION", log_level='INFO'
)

def gamelog_schedule_unification(
        data_path: Path ='pipeline_output/final/',
        file_name: str = 'nba_games_training_dataset'
        ) -> pd.DataFrame:
    """
    Unification of gamelogs and schdules dataframes.

    Args:
        data_path (Path): Path where to save the unified dataframe.
        file_name (str): Name of the unfied dataframe
    """

    # ------------------------------------------
    # Read the data

    gamelog_df = pd.concat(
        map(
            pd.read_csv,
            glob.glob(os.path.join("pipeline_output/gamelog/", "gamelog_*.csv")),
        )
    )
    schedule_df = pd.concat(
        map(
            pd.read_csv,
            glob.glob(os.path.join("pipeline_output/schedule/", "schedule_*.csv")),
        )
    )

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

    # ------------------------------------------
    # Saving final training dataset

    data_path = data_path
    file_name = file_name

    isExist = os.path.exists(data_path)
    if not isExist:
        os.makedirs(data_path)

    name_and_path_file = str(data_path) + file_name + ".csv"

    nba_games_training_dataset.to_csv(name_and_path_file, index=True)

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
        "--data-path",
        dest="data_path",
        type=Path,
        default=gamelog_schedule_unification_params["data_path"],
    )

    parser.add_argument(
        "--file-name",
        dest="file_name",
        type=str,
        default=gamelog_schedule_unification_params["file_name"],
    )

    args = parser.parse_args()

    args.data_path.parent.mkdir(
        parents=True, 
        exist_ok=True)

    return args

def main():
    """Run the Pre Train Multiple Models Pipeline."""
    args = get_args()

    gamelog_schedule_unification(
        data_path=args.data_path,
        file_name=args.file_name,
    )

if __name__ == "__main__":
    main()

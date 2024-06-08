import pandas as pd
import numpy as np
from typing import Text
import yaml
import argparse
import os
import glob
from typing import Union
from pathlib import Path

from src.utils.logs import get_logger

logger = get_logger(
    "PLAYER_ATTRIBUTES_SALARIES_UNIFICATION", log_level='INFO'
)


def player_attributes_salaries_unification(
        schedule_data_path: Path ='pipeline_output/schedule/',
        schedule_name_pattern: str = 'schedule',
        players_attributes_data_path: Path ='pipeline_output/player_attributes/',
        players_attributes_name_pattern: str = 'player_attributes',
        players_salary_data_path: Path ='pipeline_output/player_salary/',
        players_salary_name_pattern: str = 'player_salary',
        output_dest_file_path: Path ='pipeline_output/final/',
        output_file_name: str = 'player_attributes_salaries_dataset'
        ) -> None:
    """
    Unification of gamelogs and schdules dataframes.

    Args:
        schedule_data_path: Path where to read the schedule dataframe.
        schedule_name_pattern (str):  schedule name pattern to read mutliple files
        players_attributes_data_path (Path): Path where to read the player attributs dataframe.
        players_attributes_name_pattern (str): player_attributes name pattern to read mutliple files
        players_salary_data_path (Path): Path where to read the gamelog dataframe.
        players_salary_name_pattern (str):  player_salary name pattern to read mutliple files
        output_dest_file_path (Path): Path where to save the final processed dataframe.
        output_file_name (str): Name of the final processed dataframe
    """

    # ------------------------------------------
    # Read the data

    schedule_df = pd.concat(
        map(
            pd.read_csv,
            glob.glob(os.path.join(schedule_data_path, schedule_name_pattern +  "_*.csv")),
        )
    )
    players_attributes_df = pd.concat(
        map(
            pd.read_csv,
            glob.glob(os.path.join(players_attributes_data_path, players_attributes_name_pattern +  "_*.csv")),
        )
    )
    players_salary_df = pd.concat(
        map(
            pd.read_csv,
            glob.glob(os.path.join(players_salary_data_path, players_salary_name_pattern +  "_*.csv")),
        )
    )

    # Name cleaning from both player attributes and player salary
    players_attributes_df["Name"] = players_attributes_df["Name"].str.replace(
        "'", "", regex=True
    )
    players_attributes_df["Name"] = players_attributes_df["Name"].str.replace(
        "-", "", regex=True
    )
    players_attributes_df["Name"] = players_attributes_df["Name"].str.replace(
        ".", "", regex=False
    )
    players_attributes_df["Name"] = (
        players_attributes_df["Name"]
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )

    players_salary_df["name"] = players_salary_df["name"].str.replace("'", "", regex=True)
    players_salary_df["name"] = players_salary_df["name"].str.replace("-", "", regex=True)
    players_salary_df["name"] = players_salary_df["name"].str.replace(".", "", regex=False)
    players_salary_df["name"] = (
        players_salary_df["name"]
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )

    # ------------------------
    # Get the Age of player at the start of the season
    # Birth date as date
    players_attributes_df["BirthDate"] = pd.to_datetime(
        players_attributes_df["BirthDate"], format="%B %d, %Y"
    )

    gb = schedule_df.groupby(["id_season"])
    min_date_season = gb.agg({"game_date": np.min}).reset_index()

    players_attributes_df = pd.merge(
        players_attributes_df,
        min_date_season,
        how="left",
        left_on=["id_season"],
        right_on=["id_season"],
    )

    # Age at the start of the season
    players_attributes_df["Age"] = (
        pd.to_datetime(players_attributes_df["game_date"])
        - pd.to_datetime(players_attributes_df["BirthDate"])
    ).dt.days
    players_attributes_df["Age"] = players_attributes_df["Age"] / 365
    players_attributes_df["Age"] = pd.to_numeric(players_attributes_df["Age"]).round(2)

    # ------------------------
    # Get Expereince - Rename Rookie
    players_attributes_df["Experience"] = pd.to_numeric(
        np.where(
            players_attributes_df["Experience"] == "R", 0, players_attributes_df["Experience"]
        )
    )

    # ------------------------
    # Convert Ht from us sizu to cm size

    players_attributes_df[["foot", "inch"]] = players_attributes_df["Ht"].str.split(
        "-", expand=True
    )
    players_attributes_df["cm_size"] = (
        pd.to_numeric(players_attributes_df["foot"]) * 30.48
        + pd.to_numeric(players_attributes_df["inch"]) * 2.54
    )

    # ------------------------
    # Weight to numeric
    players_attributes_df["Wt"] = pd.to_numeric(players_attributes_df["Wt"])

    # ------------------------
    # Add Salary
    player_info = pd.merge(
        players_attributes_df,
        players_salary_df[["name", "year", "salary"]],
        how="left",
        left_on=["Name", "id_season"],
        right_on=["name", "year"],
    )

    # ------------------------
    # Get the columns Needed
    player_info = player_info[
        [
            "id_season",
            "tm",
            "Name",
            "Position",
            "Wt",
            "Experience",
            "Age",
            "cm_size",
            "salary",
        ]
    ]

    # ------------------------------------------
    # Saving final training dataset

    isExist = os.path.exists(output_dest_file_path)
    if not isExist:
        os.makedirs(output_dest_file_path)

    name_and_path_file = output_dest_file_path + output_file_name + ".csv"

    player_info.to_csv(name_and_path_file, index=True)

    logger.info("Player Atributes & Salaries Unification complete")

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

    player_attributes_salaries_unification = params["player_attributes_salaries_unification"]

    parser.add_argument(
        "--output-dest-file-path",
        dest="output_dest_file_path",
        type=Path,
        default=player_attributes_salaries_unification["data_path"],
    )

    parser.add_argument(
        "--output-dest-file-name",
        dest="output_dest_file_name",
        type=str,
        default=player_attributes_salaries_unification["file_name"],
    )

    player_attributes_data_acquisition = params["player_attributes_data_acquisition"]

    parser.add_argument(
        "--player-attributes-data-path",
        dest="player_attributes_data_path",
        type=Path,
        default=player_attributes_data_acquisition["output_folder"],
    )

    parser.add_argument(
        "--player-attributes-name-pattern",
        dest="player_attributes_name_pattern",
        type=str,
        default=player_attributes_data_acquisition["data_type"],
    )

    player_salary_data_acquisition = params["player_salary_data_acquisition"]

    parser.add_argument(
        "--player-salary-data-path",
        dest="player_salary_data_path",
        type=Path,
        default=player_salary_data_acquisition["output_folder"],
    )

    parser.add_argument(
        "--player-salary-name-pattern",
        dest="player_salary_name_pattern",
        type=str,
        default=player_salary_data_acquisition["data_type"],
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

    player_attributes_salaries_unification(
        schedule_data_path=args.schedule_data_path,
        schedule_name_pattern=args.schedule_name_pattern,
        players_attributes_data_path = args.players_attributes_data_path,
        players_attributes_name_pattern=args.players_attributes_name_pattern,
        players_salary_data_path=args.players_salary_data_path,
        players_salary_name_pattern=args.players_salary_name_pattern,
        output_dest_file_path=args.output_dest_file_path,
        output_file_name=args.output_file_name
    )

if __name__ == "__main__":
    main()
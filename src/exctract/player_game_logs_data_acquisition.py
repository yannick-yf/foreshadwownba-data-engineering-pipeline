import pandas as pd
from typing import Text
import yaml
import argparse
import os
from pathlib import Path
import sys
from basketball_reference_webscrapper.webscrapping_basketball_reference import (
    WebScrapBasketballReference,
)
from basketball_reference_webscrapper.data_models.feature_model import FeatureIn

from src.utils.logs import get_logger

logger = get_logger(
    "PLAYER_GAME_LOGS_DATA_ACQUISITION", log_level='INFO'
)

def player_game_logs_data_acquisition(
        data_type: str = 'player_gamelog',
        season: int = 2024,
        team: str ='all',
        output_folder: Path = 'pipeline_output/player_game_logs/'
        ) -> None:
    """
    Player game logs data acquisition.
    Args:
        data_type (str): Argument from basketball_reference_webscrapper. Type of data to pull from the package
        season (int): Argument from basketball_reference_webscrapper. Season to pull from the package
        team: Path (str): Argument from basketball_reference_webscrapper. Default is 'all'. Team to pull data from the package.
        output_folder (Path): Path where to save the player game logs data pulled using the package.
    """

    player_game_logs_df = WebScrapBasketballReference(
        FeatureIn(
            data_type=data_type,
            season=season,
            team=team,
        )
    ).webscrappe_nba_games_data()

    # ------------------------------------------
    # Saving final training dataset

    folder = output_folder

    isExist = os.path.exists(folder)
    if not isExist:
        os.makedirs(folder)

    name_and_path_file = (
        str(folder)
        +
        '/'
        + data_type
        + "_"
        + str(season)
        + "_"
        + team
        + ".csv"
    )

    player_game_logs_df.to_csv(name_and_path_file, index=False)

    logger.info("Player Game Logs Data Acquisition complete")


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

    player_game_logs_data_acquisition_params = params["player_game_logs_data_acquisition"]
    global_params = params['global_params']

    parser.add_argument(
        "--output-folder",
        dest="output_folder",
        type=Path,
        default=player_game_logs_data_acquisition_params["output_folder"],
    )

    parser.add_argument(
        "--data-type",
        dest="data_type",
        type=str,
        default=player_game_logs_data_acquisition_params["data_type"],
    )

    parser.add_argument(
        "--team",
        dest="team",
        type=str,
        default=player_game_logs_data_acquisition_params["team"],
    )

    parser.add_argument(
        "--season",
        dest="season",
        type=int,
        default=global_params["season"],
    )

    args = parser.parse_args()

    # args.unified_file_path.parent.mkdir(
    #     parents=True,
    #     exist_ok=True)

    return args

def main():
    """Run the Player Game Logs Data Acquisition Pipeline."""
    args = get_args()

    player_game_logs_data_acquisition(
        data_type=args.data_type,
        season=args.season,
        team=args.team,
        output_folder=args.output_folder,
    )

if __name__ == "__main__":
    main()

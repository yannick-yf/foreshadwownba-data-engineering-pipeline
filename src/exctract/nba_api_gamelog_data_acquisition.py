"""
NBA API Gamelog Data Acquisition Module

This module extracts game log data directly from the NBA API using the
basketball-reference-webscrapper package's WebScrapNBAApi class.

The data is extracted in Basketball Reference format (24 columns with _tm suffix)
which is compatible with the existing transformation pipeline.
"""

import pandas as pd
from typing import Text
import yaml
import argparse
import os
from pathlib import Path
import sys
from basketball_reference_webscrapper.web_scrap_nba_api import WebScrapNBAApi
from basketball_reference_webscrapper.data_models.feature_model import FeatureIn

from src.utils.logs import get_logger

logger = get_logger(
    "NBA_API_GAMELOG_DATA_ACQUISITION", log_level='INFO'
)

def nba_api_gamelog_data_acquisition(
        data_type: str = 'gamelog',
        season: int = 2024,
        team: str = 'all',
        output_folder: Path = 'pipeline_output/nba_api_gamelog/'
        ) -> None:
    """
    Extract gamelog data from NBA API.

    This function uses the WebScrapNBAApi class from basketball-reference-webscrapper
    package to extract game log data directly from the NBA API. The data is returned
    in Basketball Reference format with 24 columns (with _tm suffix).

    The output data structure is compatible with the existing transformation pipeline
    and includes the following columns:
    - id_season, game_nb, game_date, extdom, tm, opp, results
    - pts_tm, fg_tm, fga_tm, fg_prct_tm, 3p_tm, 3pa_tm, 3p_prct_tm
    - ft_tm, fta_tm, ft_prct_tm, orb_tm, trb_tm, ast_tm
    - stl_tm, blk_tm, tov_tm, pf_tm

    Args:
        data_type (str): Type of data to extract. Default is 'gamelog'.
        season (int): NBA season year to extract (e.g., 2024 for 2023-24 season).
        team (str): Team abbreviation or 'all' for all teams. Default is 'all'.
        output_folder (Path): Directory path where the CSV file will be saved.

    Returns:
        None: The function saves the extracted data as a CSV file.

    Example:
        >>> nba_api_gamelog_data_acquisition(
        ...     data_type='gamelog',
        ...     season=2024,
        ...     team='all',
        ...     output_folder='pipeline_output/nba_api_gamelog/'
        ... )
    """

    logger.info(f"Starting NBA API gamelog data acquisition for season {season}, team: {team}")

    # Create feature configuration
    feature = FeatureIn(
        data_type=data_type,
        season=season,
        team=team,
    )

    # Extract data using WebScrapNBAApi
    logger.info("Fetching data from NBA API...")
    scraper = WebScrapNBAApi(feature_object=feature)
    gamelog_df = scraper.fetch_nba_api_data()

    logger.info(f"Successfully extracted {len(gamelog_df)} records from NBA API")
    logger.info(f"Data shape: {gamelog_df.shape}")
    logger.info(f"Columns: {list(gamelog_df.columns)}")

    # Validate data structure
    expected_columns = [
        'id_season', 'game_nb', 'game_date', 'extdom', 'tm', 'opp', 'results',
        'pts_tm', 'fg_tm', 'fga_tm', 'fg_prct_tm', '3p_tm', '3pa_tm', '3p_prct_tm',
        'ft_tm', 'fta_tm', 'ft_prct_tm', 'orb_tm', 'trb_tm', 'ast_tm',
        'stl_tm', 'blk_tm', 'tov_tm', 'pf_tm'
    ]

    missing_columns = set(expected_columns) - set(gamelog_df.columns)
    if missing_columns:
        logger.warning(f"Missing expected columns: {missing_columns}")
    else:
        logger.info("✅ All expected columns are present")

    # Create output folder if it doesn't exist
    folder = Path(output_folder)
    folder.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output folder created/verified: {folder}")

    # Generate output file name
    name_and_path_file = (
        str(folder)
        + '/'
        + data_type
        + "_nba_api_"
        + str(season)
        + "_"
        + team
        + ".csv"
    )

    # Save to CSV
    gamelog_df.to_csv(name_and_path_file, index=False)
    logger.info(f"✅ Data saved to: {name_and_path_file}")
    logger.info("NBA API Gamelog Data Acquisition complete")


def get_args():
    """
    Parse command line arguments and return the parsed arguments.

    This function reads parameters from params.yaml and allows command line
    overrides for any parameter.

    Returns:
        argparse.Namespace: Parsed command line arguments with values from
                           params.yaml as defaults.
    """
    _dir = Path(__file__).parent.resolve()
    parser = argparse.ArgumentParser(
        description='Extract NBA gamelog data from NBA API'
    )

    # Load parameters from params.yaml
    parser.add_argument(
        "--params-file",
        type=Path,
        default="params.yaml",
        help="Path to parameters file"
    )

    args, _ = parser.parse_known_args()
    params = yaml.safe_load(args.params_file.open())

    # Get parameters from config file
    nba_api_params = params.get("nba_api_gamelog_data_acquisition", {})
    global_params = params.get('global_params', {})

    # Add arguments with defaults from params.yaml
    parser.add_argument(
        "--output-folder",
        dest="output_folder",
        type=Path,
        default=nba_api_params.get("output_folder", "pipeline_output/nba_api_gamelog/"),
        help="Output folder for CSV files"
    )

    parser.add_argument(
        "--data-type",
        dest="data_type",
        type=str,
        default=nba_api_params.get("data_type", "gamelog"),
        help="Type of data to extract"
    )

    parser.add_argument(
        "--team",
        dest="team",
        type=str,
        default=nba_api_params.get("team", "all"),
        help="Team abbreviation or 'all'"
    )

    parser.add_argument(
        "--season",
        dest="season",
        type=int,
        default=global_params.get("season", 2024),
        help="NBA season year"
    )

    args = parser.parse_args()

    return args


def main():
    """Run the NBA API Gamelog Data Acquisition Pipeline."""
    args = get_args()

    nba_api_gamelog_data_acquisition(
        data_type=args.data_type,
        season=args.season,
        team=args.team,
        output_folder=args.output_folder,
    )


if __name__ == "__main__":
    main()

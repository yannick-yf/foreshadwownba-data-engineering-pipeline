"""
NBA API Gamelog Transformation Module

This module transforms NBA API gamelog data by applying date filtering,
self-merging to create opponent statistics, and validating the output structure.

The transformation creates a dataset with 41 columns including both team (_tm)
and opponent (_opp) statistics for each game.
"""

import pandas as pd
import numpy as np
from typing import Text
import yaml
import argparse
import os
from pathlib import Path
from datetime import datetime

from src.utils.logs import get_logger

logger = get_logger(
    "NBA_API_GAMELOG_TRANSFORMATION", log_level='INFO'
)


def validate_columns(df: pd.DataFrame, stage: str = "output") -> bool:
    """
    Validate that the DataFrame has all required columns.

    Args:
        df (pd.DataFrame): DataFrame to validate
        stage (str): Stage of validation ("input" or "output")

    Returns:
        bool: True if all required columns are present, False otherwise
    """
    if stage == "input":
        required_columns = [
            'id_season', 'game_nb', 'game_date', 'extdom', 'tm', 'opp', 'results',
            'pts_tm', 'fg_tm', 'fga_tm', 'fg_prct_tm', '3p_tm', '3pa_tm',
            '3p_prct_tm', 'ft_tm', 'fta_tm', 'ft_prct_tm', 'orb_tm', 'trb_tm',
            'ast_tm', 'stl_tm', 'blk_tm', 'tov_tm', 'pf_tm'
        ]
    else:  # output
        required_columns = [
            'id_season', 'game_nb', 'game_date', 'extdom', 'tm', 'opp', 'results',
            'pts_tm', 'pts_opp', 'fg_tm', 'fga_tm', 'fg_prct_tm', '3p_tm', '3pa_tm',
            '3p_prct_tm', 'ft_tm', 'fta_tm', 'ft_prct_tm', 'orb_tm', 'trb_tm', 'ast_tm',
            'stl_tm', 'blk_tm', 'tov_tm', 'pf_tm', 'fg_opp', 'fga_opp', 'fg_prct_opp',
            '3p_opp', '3pa_opp', '3p_prct_opp', 'ft_opp', 'fta_opp', 'ft_prct_opp',
            'orb_opp', 'trb_opp', 'ast_opp', 'stl_opp', 'blk_opp', 'tov_opp', 'pf_opp'
        ]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        logger.error(f"❌ Missing {len(missing_columns)} column(s) at {stage} stage:")
        for col in missing_columns:
            logger.error(f"   - {col}")
        return False
    else:
        logger.info(f"✅ All {len(required_columns)} required columns are present at {stage} stage")
        return True


def nba_api_gamelog_transformation(
        input_file: Path,
        output_folder: Path = 'pipeline_output/nba_api_transformed/',
        season: int = 2024,
        season_start_date: str = None,
        season_end_date: str = None
        ) -> None:
    """
    Transform NBA API gamelog data by filtering, merging, and validating.

    This function:
    1. Loads NBA API gamelog data from CSV
    2. Validates input structure (24 columns with _tm suffix)
    3. Filters games by season date range
    4. Self-merges data to create opponent statistics (_opp columns)
    5. Validates output structure (41 columns with _tm and _opp)
    6. Saves transformed data to CSV

    The self-merge operation pairs each game's team statistics with the
    opponent's statistics from the same game, creating a comprehensive
    dataset with both perspectives of each game.

    Args:
        input_file (Path): Path to input CSV file with NBA API gamelog data
        output_folder (Path): Directory where transformed data will be saved
        season (int): NBA season year for output file naming
        season_start_date (str): Start date for filtering (YYYY-MM-DD format)
        season_end_date (str): End date for filtering (YYYY-MM-DD format)

    Returns:
        None: The function saves the transformed data as a CSV file.

    Example:
        >>> nba_api_gamelog_transformation(
        ...     input_file='pipeline_output/nba_api_gamelog/gamelog_nba_api_2024_all.csv',
        ...     output_folder='pipeline_output/nba_api_transformed/',
        ...     season=2024,
        ...     season_start_date='2023-10-01',
        ...     season_end_date='2024-06-30'
        ... )
    """

    logger.info(f"Starting NBA API gamelog transformation for season {season}")
    logger.info(f"Input file: {input_file}")

    # Load data - keep all columns as strings to preserve format (e.g., .422 instead of 0.422)
    logger.info("Loading gamelog data...")
    gamelog = pd.read_csv(input_file, dtype=str)
    logger.info(f"Loaded {len(gamelog)} records")
    logger.info(f"Input data shape: {gamelog.shape}")

    # Validate input structure
    logger.info("Validating input data structure...")
    if not validate_columns(gamelog, stage="input"):
        raise ValueError("Input data is missing required columns")

    # Convert game_date to datetime
    logger.info("Converting game_date to datetime...")
    gamelog['game_date'] = pd.to_datetime(gamelog['game_date'])

    # Filter by season date range if provided
    if season_start_date and season_end_date:
        logger.info(f"Filtering games between {season_start_date} and {season_end_date}")
        games = gamelog[
            (pd.to_datetime(gamelog['game_date']) >= pd.to_datetime(season_start_date)) &
            (pd.to_datetime(gamelog['game_date']) <= pd.to_datetime(season_end_date))
        ]
        logger.info(f"Filtered to {len(games)} games in date range")
    else:
        logger.info("No date filtering applied")
        games = gamelog.copy()

    if len(games) == 0:
        logger.warning("⚠️  No games found after filtering. Creating empty output.")
        # Create empty dataframe with correct columns
        required_columns = [
            'id_season', 'game_nb', 'game_date', 'extdom', 'tm', 'opp', 'results',
            'pts_tm', 'pts_opp', 'fg_tm', 'fga_tm', 'fg_prct_tm', '3p_tm', '3pa_tm',
            '3p_prct_tm', 'ft_tm', 'fta_tm', 'ft_prct_tm', 'orb_tm', 'trb_tm', 'ast_tm',
            'stl_tm', 'blk_tm', 'tov_tm', 'pf_tm', 'fg_opp', 'fga_opp', 'fg_prct_opp',
            '3p_opp', '3pa_opp', '3p_prct_opp', 'ft_opp', 'fta_opp', 'ft_prct_opp',
            'orb_opp', 'trb_opp', 'ast_opp', 'stl_opp', 'blk_opp', 'tov_opp', 'pf_opp'
        ]
        test_merge = pd.DataFrame(columns=required_columns)
    else:
        # Remove '_tm' suffix from column names (except pts_tm and stat columns)
        # This is needed for the merge operation
        logger.info("Preparing data for self-merge...")
        games.columns = games.columns.str.replace('_tm', '', regex=False)

        # Self-merge: Join games with themselves to get opponent statistics
        logger.info("Performing self-merge to create opponent statistics...")
        logger.info("Merging on: game_date, tm (left) with opp (right)")

        test_merge = pd.merge(
            games,
            games,
            left_on=['game_date', 'tm'],
            right_on=['game_date', 'opp'],
            suffixes=('_tm', '_opp')
        )

        logger.info(f"Merge complete. Result shape: {test_merge.shape}")

        # Drop redundant opponent columns
        logger.info("Dropping redundant columns...")
        columns_to_drop = ['id_season_opp', 'game_nb_opp', 'tm_opp', 'opp_opp', 'results_opp', 'extdom_opp']
        # Only drop columns that exist
        columns_to_drop = [col for col in columns_to_drop if col in test_merge.columns]
        test_merge = test_merge.drop(columns=columns_to_drop)

        # Rename columns to standard format
        logger.info("Renaming columns to standard format...")
        rename_map = {
            'id_season_tm': 'id_season',
            'game_nb_tm': 'game_nb',
            'tm_tm': 'tm',
            'opp_tm': 'opp',
            'results_tm': 'results',
            'extdom_tm': 'extdom',
        }
        test_merge = test_merge.rename(columns=rename_map)

        # Handle extdom column (replace empty strings with NaN)
        logger.info("Processing extdom column...")
        test_merge['extdom'] = np.where(
            test_merge['extdom'] == '',
            np.nan,
            test_merge['extdom']
        )

        logger.info(f"Transformation complete. Final shape: {test_merge.shape}")

    # Validate output structure
    logger.info("Validating output data structure...")
    if not validate_columns(test_merge, stage="output"):
        raise ValueError("Output data is missing required columns")

    # Create output folder
    folder = Path(output_folder)
    folder.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output folder created/verified: {folder}")

    # Generate output file name
    output_file = folder / f"gamelog_nba_api_transformed_{season}.csv"

    # Save to CSV
    test_merge.to_csv(output_file, index=False)
    logger.info(f"✅ Transformed data saved to: {output_file}")
    logger.info(f"✅ Final dataset contains {len(test_merge)} records with 41 columns")
    logger.info("NBA API Gamelog Transformation complete")


def get_args():
    """
    Parse command line arguments and return the parsed arguments.

    Returns:
        argparse.Namespace: Parsed command line arguments.
    """
    parser = argparse.ArgumentParser(
        description='Transform NBA API gamelog data'
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
    nba_api_transform_params = params.get("nba_api_gamelog_transformation", {})
    global_params = params.get('global_params', {})

    # Add arguments
    parser.add_argument(
        "--input-file",
        dest="input_file",
        type=Path,
        default=nba_api_transform_params.get("input_file"),
        required=True,
        help="Input CSV file with NBA API gamelog data"
    )

    parser.add_argument(
        "--output-folder",
        dest="output_folder",
        type=Path,
        default=nba_api_transform_params.get("output_folder", "pipeline_output/nba_api_transformed/"),
        help="Output folder for transformed data"
    )

    parser.add_argument(
        "--season",
        dest="season",
        type=int,
        default=global_params.get("season", 2024),
        help="NBA season year"
    )

    parser.add_argument(
        "--season-start-date",
        dest="season_start_date",
        type=str,
        default=nba_api_transform_params.get("season_start_date"),
        help="Season start date (YYYY-MM-DD)"
    )

    parser.add_argument(
        "--season-end-date",
        dest="season_end_date",
        type=str,
        default=nba_api_transform_params.get("season_end_date"),
        help="Season end date (YYYY-MM-DD)"
    )

    args = parser.parse_args()

    return args


def main():
    """Run the NBA API Gamelog Transformation Pipeline."""
    args = get_args()

    nba_api_gamelog_transformation(
        input_file=args.input_file,
        output_folder=args.output_folder,
        season=args.season,
        season_start_date=args.season_start_date,
        season_end_date=args.season_end_date,
    )


if __name__ == "__main__":
    main()

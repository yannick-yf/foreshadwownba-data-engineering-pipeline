import pandas as pd
from typing import Text
import yaml
import argparse
import os
from pathlib import Path
import sys
import requests
import time
import json
from src.utils.logs import get_logger

logger = get_logger(
    "PLAYER_GAMELOGS_DATA_ACQUISITION", log_level='INFO'
)

def player_gamelogs_data_acquisition(
        data_type: str = 'player_gamelogs',
        season: int = 2024,
        output_folder: Path = 'pipeline_output/player_gamelogs/',
        players_list: dict =  None
        ) -> None:
    """
    Player Gamelog data acquisition.
    Args:
        data_type (str): Type of data to pull - player_gamelogs
        season (int): Season to pull from the NBA API
        output_folder (Path): Path where to save the gamelog data pulled using the NBA API.
        players (dict) - Optional: List of specifc players to get gamelogs data
    """
    
    logger.info(f"Starting Player Gamelogs Data Acquisition for season {season}")
    
    # Convert season to NBA API format (e.g., 2025 -> "2024-25")
    nba_season = f"{season-1}-{str(season)[-2:]}"
    
    # ------------------------------------------
    # Get list of NBA players for the given season
    if players_list:
        players = players_list
    else:
        logger.info(f"Fetching NBA players list for {nba_season}...")
        players = get_all_nba_players(nba_season)
        
        if not players:
            logger.error("Failed to fetch NBA players. Exiting.")
            return
        
        logger.info(f"Found {len(players)} players")

    # ------------------------------------------
    # For each player, get all the gamelogs
    all_gamelogs = []
    
    for i, (player_name, player_id) in enumerate(players.items()):
        logger.info(f"Processing player {i+1}/{len(players)}: {player_name}")
        
        player_gamelog_df = get_player_game_log(player_id, nba_season)
        
        if not player_gamelog_df.empty:
            # Add player name to the dataframe
            player_gamelog_df['PLAYER_NAME'] = player_name
            all_gamelogs.append(player_gamelog_df)
        
        # Add delay to avoid rate limiting
        time.sleep(0.5)
    
    # ------------------------------------------
    # Combine all player gamelogs into one dataframe
    if all_gamelogs:
        player_gamelogs_df = pd.concat(all_gamelogs, ignore_index=True)
        logger.info(f"Combined gamelogs for {len(all_gamelogs)} players")
    else:
        logger.warning("No gamelog data found for any players")
        player_gamelogs_df = pd.DataFrame()
    
    # ------------------------------------------
    # Save the file
    folder = output_folder
    
    isExist = os.path.exists(folder)
    if not isExist:
        os.makedirs(folder)
    
    name_and_path_file = (
        str(folder)
        + '/'
        + data_type
        + "_"
        + str(season)
        + "_all.csv"
    )
    
    player_gamelogs_df.to_csv(name_and_path_file, index=False)
    
    logger.info(f"Player Gamelogs Data Acquisition complete. Saved to: {name_and_path_file}")


def get_all_nba_players(season="2024-25"):
    """
    Fetches all NBA player IDs using the official NBA API.
    Returns a dictionary with player names as keys and their IDs as values.
    
    Args:
        season (str): The NBA season in format "YYYY-YY" (e.g., "2024-25")
        
    Returns:
        dict: Dictionary with player names as keys and player IDs as values
    """
    url = "https://stats.nba.com/stats/commonallplayers"
    
    # Headers to mimic a browser request (NBA API requires this)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'https://www.nba.com/',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive'
    }
    
    # Parameters for the API request
    params = {
        'LeagueID': '00',  # 00 is the NBA
        'Season': season,
        'IsOnlyCurrentSeason': 1
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        # Parse the JSON response
        data = response.json()
        
        players = {}
        if data.get('resultSets') and len(data['resultSets']) > 0:
            result_set = data['resultSets'][0]
            headers_list = result_set.get('headers', [])
            rows = result_set.get('rowSet', [])
            
            # Find indices for player ID and name
            try:
                player_id_index = headers_list.index('PERSON_ID')
                player_name_index = headers_list.index('DISPLAY_FIRST_LAST')
                
                for row in rows:
                    player_id = row[player_id_index]
                    player_name = row[player_name_index]
                    players[player_name] = player_id
                
                return players
            except ValueError as e:
                logger.error(f"Could not find expected columns: {e}")
                logger.error(f"Available columns: {headers_list}")
                return {}
        
        return {}
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching player data: {e}")
        return {}


def get_player_game_log(player_id, season="2024-25", season_type="Regular Season"):
    """
    Fetches game log statistics for a specific player using the NBA API.
    
    Args:
        player_id (int): The player's ID
        season (str): The NBA season in format "YYYY-YY" (e.g., "2024-25")
        season_type (str): "Regular Season", "Playoffs", "All-Star", etc.
        
    Returns:
        pandas.DataFrame: DataFrame containing the player's game log statistics
    """
    # NBA API endpoint for player game logs
    url = "https://stats.nba.com/stats/playergamelog"
    
    # Headers to mimic a browser request (NBA API requires this)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'https://www.nba.com/',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive'
    }
    
    # Parameters for the API request
    params = {
        'PlayerID': player_id,
        'Season': season,
        'SeasonType': season_type,
        'DateFrom': '',
        'DateTo': ''
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        # Parse the JSON response
        data = response.json()
        
        # Extract headers and rows
        result_set = data['resultSets'][0]
        headers_list = result_set['headers']
        rows = result_set['rowSet']
        
        # Create a pandas DataFrame
        df = pd.DataFrame(rows, columns=headers_list)
        
        return df
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching game log data for player {player_id}: {e}")
        return pd.DataFrame()


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

    player_gamelogs_data_acquisition_params = params["player_gamelogs_data_acquisition"]
    global_params = params['global_params']

    parser.add_argument(
        "--output-folder",
        dest="output_folder",
        type=Path,
        default=player_gamelogs_data_acquisition_params["output_folder"],
    )

    parser.add_argument(
        "--data-type",
        dest="data_type",
        type=str,
        default=player_gamelogs_data_acquisition_params["data_type"],
    )

    parser.add_argument(
        "--season",
        dest="season",
        type=int,
        default=global_params["season"],
    )

    parser.add_argument(
        "--players-list",
        dest="players_list",
        type=dict,
        default=None,
    )

    args = parser.parse_args()

    return args


def main():
    """Run the Player Gamelogs Data Acquisition Pipeline."""
    args = get_args()

    player_gamelogs_data_acquisition(
        data_type=args.data_type,
        season=args.season,
        output_folder=args.output_folder,
        players_list=args.players_list
    )


if __name__ == "__main__":
    main()
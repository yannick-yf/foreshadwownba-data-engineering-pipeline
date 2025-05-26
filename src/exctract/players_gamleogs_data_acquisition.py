import requests
import json
import time

def get_all_nba_players():
    """
    Fetches all NBA player IDs using the official NBA API.
    Returns a dictionary with player names as keys and their IDs as values.
    """
    # NBA API endpoint for all players
    url = "https://stats.nba.com/stats/playerindex"
    
    # Headers to mimic a browser request (NBA API requires this)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'https://www.nba.com/',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive'
    }
    
    # Parameters for the API request
    params = {
        'Historical': 0,  # 0 for active players, 1 for all players including historical
        'LeagueID': '00',  # 00 is the NBA
        'Season': '2024-25'  # Current season
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # Parse the JSON response
        data = response.json()
        
        # Debug: Print the actual structure of the response
        print("Response structure:")
        for i, result_set in enumerate(data.get('resultSets', [])):
            print(f"Result set {i}:")
            print(f"Name: {result_set.get('name')}")
            print(f"Headers: {result_set.get('headers')}")
            print(f"Row count: {len(result_set.get('rowSet', []))}")
            print("---")
        
        # Try to extract players from the first result set
        players = {}
        if data.get('resultSets') and len(data['resultSets']) > 0:
            result_set = data['resultSets'][0]
            headers = result_set.get('headers', [])
            rows = result_set.get('rowSet', [])
            
            # Find indices for player ID and name
            player_id_index = None
            player_name_index = None
            
            # Look for common column names for player ID and name
            for i, header in enumerate(headers):
                if header in ['PERSON_ID', 'PLAYER_ID']:
                    player_id_index = i
                if header in ['PLAYER_NAME', 'DISPLAY_FIRST_LAST', 'DISPLAY_LAST_COMMA_FIRST']:
                    player_name_index = i
            
            # If we found both indices, extract player data
            if player_id_index is not None and player_name_index is not None:
                for row in rows:
                    player_id = row[player_id_index]
                    player_name = row[player_name_index]
                    players[player_name] = player_id
            else:
                print("Could not find player ID and name columns in the response")
                print(f"Available columns: {headers}")
        
        return players
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching player data: {e}")
        return {}

def try_alternative_endpoint():
    """
    Try an alternative NBA API endpoint to get player data.
    """
    url = "https://stats.nba.com/stats/commonallplayers"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'https://www.nba.com/',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive'
    }
    
    params = {
        'LeagueID': '00',
        'Season': '2024-25',
        'IsOnlyCurrentSeason': 1
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        players = {}
        if data.get('resultSets') and len(data['resultSets']) > 0:
            result_set = data['resultSets'][0]
            headers = result_set.get('headers', [])
            rows = result_set.get('rowSet', [])
            
            # Find indices for player ID and name
            try:
                player_id_index = headers.index('PERSON_ID')
                player_name_index = headers.index('DISPLAY_FIRST_LAST')
                
                for row in rows:
                    player_id = row[player_id_index]
                    player_name = row[player_name_index]
                    players[player_name] = player_id
                
                return players
            except ValueError:
                print("Could not find expected columns in alternative endpoint")
                print(f"Available columns: {headers}")
                return {}
        
        return {}
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching player data from alternative endpoint: {e}")
        return {}

def save_players_to_file(players, filename='nba_players.json'):
    """
    Saves player data to a JSON file.
    """
    with open(filename, 'w') as f:
        json.dump(players, f, indent=4)
    print(f"Player data saved to {filename}")

def main():
    print("Fetching NBA player IDs...")
    players = get_all_nba_players()
    
    # If the first attempt fails, try the alternative endpoint
    if not players:
        print("First endpoint failed, trying alternative endpoint...")
        players = try_alternative_endpoint()
    
    if players:
        print(f"Found {len(players)} players")
        save_players_to_file(players)
        
        # Print a sample of players
        print("\nSample of player IDs:")
        sample_players = list(players.items())[:5]
        for name, player_id in sample_players:
            print(f"{name}: {player_id}")
    else:
        print("No player data retrieved")

if __name__ == "__main__":
    main()
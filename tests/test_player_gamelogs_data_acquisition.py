import pytest
import pandas as pd
import os
import tempfile
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path
import requests

from exctract.player_gamelogs_data_acquisition import player_gamelogs_data_acquisition, get_all_nba_players, get_player_game_log

class TestPlayerGamelogsDataAcquisition:
    """Test suite for player gamelogs data acquisition functions."""

    def test_player_gamelogs_data_acquisition_success_one_player(
        self
    ):
        """Test successful player gamelogs data acquisition."""

        file_path = "tests/test_output/player_gamelogs_2024_all.csv"

        if os.path.exists(file_path):
            os.remove(file_path)
        
        # Run the function
        player_gamelogs_data_acquisition(
            data_type='player_gamelogs',
            season=2024,
            output_folder="tests/test_output",
            players_list={'LeBron James':2544}
        )
        
        assert os.path.exists(file_path), f"Expected output file does not exist: {file_path}"

        if os.path.exists(file_path):
            os.remove(file_path)
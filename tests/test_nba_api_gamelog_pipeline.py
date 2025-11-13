"""
Test NBA API Gamelog Pipeline

This module tests the complete NBA API gamelog pipeline including:
1. Data extraction from NBA API
2. Transformation logic (filtering, merging, validation)
3. Output validation for specific games (ATL-TOR on 2025-10-22)
"""

import unittest
import pandas as pd
import numpy as np
from pathlib import Path
import shutil
import os

from src.exctract.nba_api_gamelog_data_acquisition import nba_api_gamelog_data_acquisition
from src.transform.nba_api_gamelog_transformation import (
    nba_api_gamelog_transformation,
    validate_columns
)


class TestNBAAPIGamelogPipeline(unittest.TestCase):
    """Test suite for NBA API gamelog pipeline."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures - create test output directories."""
        cls.test_output_dir = Path("./tests/test_output/nba_api")
        cls.test_output_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def tearDownClass(cls):
        """Clean up test fixtures - remove test output directories."""
        if cls.test_output_dir.exists():
            shutil.rmtree(cls.test_output_dir)

    def test_01_column_validation_input(self):
        """Test input column validation with correct structure."""
        # Create sample data with correct input structure (24 columns with _tm suffix)
        data = {
            'id_season': ['2026'],
            'game_nb': ['1'],
            'game_date': ['2025-10-22'],
            'extdom': [''],
            'tm': ['ATL'],
            'opp': ['TOR'],
            'results': ['L'],
            'pts_tm': ['118'],
            'fg_tm': ['38'],
            'fga_tm': ['90'],
            'fg_prct_tm': ['.422'],
            '3p_tm': ['10'],
            '3pa_tm': ['35'],
            '3p_prct_tm': ['.286'],
            'ft_tm': ['32'],
            'fta_tm': ['37'],
            'ft_prct_tm': ['.865'],
            'orb_tm': ['8'],
            'trb_tm': ['34'],
            'ast_tm': ['25'],
            'stl_tm': ['7'],
            'blk_tm': ['6'],
            'tov_tm': ['16'],
            'pf_tm': ['24']
        }

        df = pd.DataFrame(data)
        result = validate_columns(df, stage="input")
        self.assertTrue(result, "Input validation should pass with all required columns")

    def test_02_column_validation_output(self):
        """Test output column validation with correct structure."""
        # Create sample data with correct output structure (41 columns)
        data = {
            'id_season': ['2026'],
            'game_nb': ['1'],
            'game_date': ['2025-10-22'],
            'extdom': [np.nan],
            'tm': ['ATL'],
            'opp': ['TOR'],
            'results': ['L'],
            'pts_tm': ['118'],
            'pts_opp': ['138'],
            'fg_tm': ['38'],
            'fga_tm': ['90'],
            'fg_prct_tm': ['.422'],
            '3p_tm': ['10'],
            '3pa_tm': ['35'],
            '3p_prct_tm': ['.286'],
            'ft_tm': ['32'],
            'fta_tm': ['37'],
            'ft_prct_tm': ['.865'],
            'orb_tm': ['8'],
            'trb_tm': ['34'],
            'ast_tm': ['25'],
            'stl_tm': ['7'],
            'blk_tm': ['6'],
            'tov_tm': ['16'],
            'pf_tm': ['24'],
            'fg_opp': ['54'],
            'fga_opp': ['95'],
            'fg_prct_opp': ['.568'],
            '3p_opp': ['6'],
            '3pa_opp': ['25'],
            '3p_prct_opp': ['.240'],
            'ft_opp': ['24'],
            'fta_opp': ['29'],
            'ft_prct_opp': ['.828'],
            'orb_opp': ['12'],
            'trb_opp': ['54'],
            'ast_opp': ['36'],
            'stl_opp': ['10'],
            'blk_opp': ['4'],
            'tov_opp': ['19'],
            'pf_opp': ['31']
        }

        df = pd.DataFrame(data)
        result = validate_columns(df, stage="output")
        self.assertTrue(result, "Output validation should pass with all required columns")

    def test_03_transformation_logic_atl_tor_game(self):
        """Test transformation logic with ATL-TOR game on 2025-10-22."""
        # Create test input data for both teams' perspectives of the game
        input_data = pd.DataFrame({
            'id_season': ['2026', '2026'],
            'game_nb': ['1', '1'],
            'game_date': ['2025-10-22', '2025-10-22'],
            'extdom': ['', ''],
            'tm': ['ATL', 'TOR'],
            'opp': ['TOR', 'ATL'],
            'results': ['L', 'W'],
            'pts_tm': ['118', '138'],
            'fg_tm': ['38', '54'],
            'fga_tm': ['90', '95'],
            'fg_prct_tm': ['.422', '.568'],
            '3p_tm': ['10', '6'],
            '3pa_tm': ['35', '25'],
            '3p_prct_tm': ['.286', '.240'],
            'ft_tm': ['32', '24'],
            'fta_tm': ['37', '29'],
            'ft_prct_tm': ['.865', '.828'],
            'orb_tm': ['8', '12'],
            'trb_tm': ['34', '54'],
            'ast_tm': ['25', '36'],
            'stl_tm': ['7', '10'],
            'blk_tm': ['6', '4'],
            'tov_tm': ['16', '19'],
            'pf_tm': ['24', '31']
        })

        # Save test input
        test_input_file = self.test_output_dir / "test_input_atl_tor.csv"
        input_data.to_csv(test_input_file, index=False)

        # Run transformation
        test_output_folder = self.test_output_dir / "transformed"
        nba_api_gamelog_transformation(
            input_file=test_input_file,
            output_folder=test_output_folder,
            season=2026,
            season_start_date='2025-10-01',
            season_end_date='2026-06-30'
        )

        # Load transformed data - preserve string format (dtype=str)
        output_file = test_output_folder / "gamelog_nba_api_transformed_2026.csv"
        self.assertTrue(output_file.exists(), "Output file should be created")

        result = pd.read_csv(output_file, dtype=str)

        # Validate structure
        self.assertTrue(validate_columns(result, stage="output"),
                       "Output should have all 41 required columns")

        # Validate ATL game data
        atl_game = result[result['tm'] == 'ATL'].iloc[0]

        # Expected values for ATL-TOR game (ATL perspective)
        expected = {
            'id_season': '2026',
            'game_nb': '1',
            'tm': 'ATL',
            'opp': 'TOR',
            'results': 'L',
            'pts_tm': '118',
            'pts_opp': '138',
            'fg_tm': '38',
            'fga_tm': '90',
            'fg_prct_tm': '.422',
            '3p_tm': '10',
            '3pa_tm': '35',
            '3p_prct_tm': '.286',
            'ft_tm': '32',
            'fta_tm': '37',
            'ft_prct_tm': '.865',
            'orb_tm': '8',
            'trb_tm': '34',
            'ast_tm': '25',
            'stl_tm': '7',
            'blk_tm': '6',
            'tov_tm': '16',
            'pf_tm': '24',
            'fg_opp': '54',
            'fga_opp': '95',
            'fg_prct_opp': '.568',
            '3p_opp': '6',
            '3pa_opp': '25',
            '3p_prct_opp': '.240',
            'ft_opp': '24',
            'fta_opp': '29',
            'ft_prct_opp': '.828',
            'orb_opp': '12',
            'trb_opp': '54',
            'ast_opp': '36',
            'stl_opp': '10',
            'blk_opp': '4',
            'tov_opp': '19',
            'pf_opp': '31'
        }

        # Validate each field
        for key, expected_value in expected.items():
            actual_value = str(atl_game[key])
            self.assertEqual(actual_value, expected_value,
                           f"Field {key} should match expected value")

        print("\n✅ ATL-TOR Game Validation Passed!")
        print(f"   Game Date: {atl_game['game_date']}")
        print(f"   Matchup: {atl_game['tm']} vs {atl_game['opp']}")
        print(f"   Score: {atl_game['pts_tm']}-{atl_game['pts_opp']}")
        print(f"   Result: {atl_game['results']}")

    def test_04_csv_output_format(self):
        """Test CSV output format matches expected structure."""
        # Create test data
        input_data = pd.DataFrame({
            'id_season': ['2026', '2026'],
            'game_nb': ['1', '1'],
            'game_date': ['2025-10-22', '2025-10-22'],
            'extdom': ['', ''],
            'tm': ['ATL', 'TOR'],
            'opp': ['TOR', 'ATL'],
            'results': ['L', 'W'],
            'pts_tm': ['118', '138'],
            'fg_tm': ['38', '54'],
            'fga_tm': ['90', '95'],
            'fg_prct_tm': ['.422', '.568'],
            '3p_tm': ['10', '6'],
            '3pa_tm': ['35', '25'],
            '3p_prct_tm': ['.286', '.240'],
            'ft_tm': ['32', '24'],
            'fta_tm': ['37', '29'],
            'ft_prct_tm': ['.865', '.828'],
            'orb_tm': ['8', '12'],
            'trb_tm': ['34', '54'],
            'ast_tm': ['25', '36'],
            'stl_tm': ['7', '10'],
            'blk_tm': ['6', '4'],
            'tov_tm': ['16', '19'],
            'pf_tm': ['24', '31']
        })

        test_input_file = self.test_output_dir / "test_csv_format.csv"
        input_data.to_csv(test_input_file, index=False)

        # Run transformation
        test_output_folder = self.test_output_dir / "csv_test"
        nba_api_gamelog_transformation(
            input_file=test_input_file,
            output_folder=test_output_folder,
            season=2026,
            season_start_date='2025-10-01',
            season_end_date='2026-06-30'
        )

        # Read output file - preserve string format (dtype=str)
        output_file = test_output_folder / "gamelog_nba_api_transformed_2026.csv"
        result = pd.read_csv(output_file, dtype=str)

        # Get ATL game row
        atl_row = result[result['tm'] == 'ATL'].iloc[0]

        # Expected CSV row (without header)
        expected_csv_row = "2026,1,2025-10-22,,ATL,TOR,L,118,138,38,90,.422,10,35,.286,32,37,.865,8,34,25,7,6,16,24,54,95,.568,6,25,.240,24,29,.828,12,54,36,10,4,19,31"

        # Build actual CSV row
        actual_csv_row = ','.join([
            str(atl_row['id_season']),
            str(atl_row['game_nb']),
            str(atl_row['game_date']),
            '' if pd.isna(atl_row['extdom']) else str(atl_row['extdom']),
            str(atl_row['tm']),
            str(atl_row['opp']),
            str(atl_row['results']),
            str(atl_row['pts_tm']),
            str(atl_row['pts_opp']),
            str(atl_row['fg_tm']),
            str(atl_row['fga_tm']),
            str(atl_row['fg_prct_tm']),
            str(atl_row['3p_tm']),
            str(atl_row['3pa_tm']),
            str(atl_row['3p_prct_tm']),
            str(atl_row['ft_tm']),
            str(atl_row['fta_tm']),
            str(atl_row['ft_prct_tm']),
            str(atl_row['orb_tm']),
            str(atl_row['trb_tm']),
            str(atl_row['ast_tm']),
            str(atl_row['stl_tm']),
            str(atl_row['blk_tm']),
            str(atl_row['tov_tm']),
            str(atl_row['pf_tm']),
            str(atl_row['fg_opp']),
            str(atl_row['fga_opp']),
            str(atl_row['fg_prct_opp']),
            str(atl_row['3p_opp']),
            str(atl_row['3pa_opp']),
            str(atl_row['3p_prct_opp']),
            str(atl_row['ft_opp']),
            str(atl_row['fta_opp']),
            str(atl_row['ft_prct_opp']),
            str(atl_row['orb_opp']),
            str(atl_row['trb_opp']),
            str(atl_row['ast_opp']),
            str(atl_row['stl_opp']),
            str(atl_row['blk_opp']),
            str(atl_row['tov_opp']),
            str(atl_row['pf_opp'])
        ])

        self.assertEqual(actual_csv_row, expected_csv_row,
                        "CSV output row should match expected format exactly")

        print("\n✅ CSV Format Validation Passed!")
        print(f"   Expected: {expected_csv_row}")
        print(f"   Actual:   {actual_csv_row}")

    def test_05_date_filtering(self):
        """Test date filtering logic."""
        # Create test data with games outside date range
        input_data = pd.DataFrame({
            'id_season': ['2026', '2026', '2026', '2026'],
            'game_nb': ['1', '1', '2', '2'],
            'game_date': ['2025-09-30', '2025-09-30', '2025-10-22', '2025-10-22'],  # First game outside range
            'extdom': ['', '', '', ''],
            'tm': ['ATL', 'TOR', 'ATL', 'TOR'],
            'opp': ['TOR', 'ATL', 'TOR', 'ATL'],
            'results': ['L', 'W', 'L', 'W'],
            'pts_tm': ['100', '105', '118', '138'],
            'fg_tm': ['38', '40', '38', '54'],
            'fga_tm': ['90', '88', '90', '95'],
            'fg_prct_tm': ['.422', '.455', '.422', '.568'],
            '3p_tm': ['10', '8', '10', '6'],
            '3pa_tm': ['35', '30', '35', '25'],
            '3p_prct_tm': ['.286', '.267', '.286', '.240'],
            'ft_tm': ['24', '25', '32', '24'],
            'fta_tm': ['28', '30', '37', '29'],
            'ft_prct_tm': ['.857', '.833', '.865', '.828'],
            'orb_tm': ['8', '10', '8', '12'],
            'trb_tm': ['34', '38', '34', '54'],
            'ast_tm': ['25', '28', '25', '36'],
            'stl_tm': ['7', '8', '7', '10'],
            'blk_tm': ['6', '5', '6', '4'],
            'tov_tm': ['16', '18', '16', '19'],
            'pf_tm': ['24', '26', '24', '31']
        })

        test_input_file = self.test_output_dir / "test_date_filter.csv"
        input_data.to_csv(test_input_file, index=False)

        # Run transformation with date filter
        test_output_folder = self.test_output_dir / "date_filter_test"
        nba_api_gamelog_transformation(
            input_file=test_input_file,
            output_folder=test_output_folder,
            season=2026,
            season_start_date='2025-10-01',
            season_end_date='2026-06-30'
        )

        # Load transformed data
        output_file = test_output_folder / "gamelog_nba_api_transformed_2026.csv"
        result = pd.read_csv(output_file)

        # Should only have the October 22 game (2 rows: ATL and TOR perspectives)
        self.assertEqual(len(result), 2, "Should only have 2 rows (filtered game)")

        # Verify dates are within range
        result['game_date'] = pd.to_datetime(result['game_date'])
        all_dates_valid = all(
            (result['game_date'] >= pd.to_datetime('2025-10-01')) &
            (result['game_date'] <= pd.to_datetime('2026-06-30'))
        )
        self.assertTrue(all_dates_valid, "All dates should be within specified range")

        print("\n✅ Date Filtering Validation Passed!")
        print(f"   Input games: 2 (4 rows)")
        print(f"   Filtered games: 1 (2 rows)")
        print(f"   Date range: 2025-10-01 to 2026-06-30")


def run_tests():
    """Run all tests and print summary."""
    suite = unittest.TestLoader().loadTestsFromTestCase(TestNBAAPIGamelogPipeline)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")

    if result.wasSuccessful():
        print("\n✅ All tests passed!")
    else:
        print("\n❌ Some tests failed!")

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    exit(0 if success else 1)

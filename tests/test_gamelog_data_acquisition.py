from unittest import TestCase
import os
from src.exctract import gamelog_data_acquisition
import shutil

class TestGamelogDataAcquisition(TestCase):
    def setUp(self) -> None:
        self.data_type = 'gamelog'
        self.season = 2024
        self.team = 'ATL'
        self.output_folder = 'tests/test_output'

    def test_gamelog_data_acquisition_w_all_args(self):

        gamelog_data_acquisition.gamelog_data_acquisition(
                data_type=self.data_type,
                season=self.season,
                team=self.team,
                output_folder=self.output_folder,
        )

        unified_file_path_name = (
            str(self.output_folder)
            +
            '/'
            + self.data_type
            + "_"
            + str(self.season)
            + "_"
            + self.team
            + ".csv"
        )

        assert os.path.exists(unified_file_path_name)

    def test_gamelog_data_acquisition_w_team_args(self):

        gamelog_data_acquisition.gamelog_data_acquisition(
            team = 'ATL'
        )

        unified_file_path_name = (
            'pipeline_output/gamelog'
            +
            '/'
            + 'gamelog'
            + "_"
            + '2024'
            + "_"
            + 'ATL'
            + ".csv"
        )

        assert os.path.exists(unified_file_path_name)
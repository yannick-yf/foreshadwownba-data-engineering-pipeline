from unittest import TestCase
import os
from src.transform import player_attributes_salaries_unification
import shutil

class TestFeatureSelection(TestCase):
    def setUp(self) -> None:
        self.output_dest_file_path = './tests/test_output/'
        self.output_file_name = 'player_attributes_salaries_dataset'
        self.gamelog_data_path = 'pipeline_output/gamelog/'
        self.gamelog_name_pattern = 'gamelog'
        self.schedule_data_path = 'pipeline_output/schedule/'
        self.schedule_name_pattern = 'schedule'

    def test_player_attributes_salaries_unification_wo_arguments(self):

        # Check if the file exists before attempting to delete it
        if os.path.exists(self.output_dest_file_path + self.output_file_name + '.csv'):
            shutil.rmtree(self.output_dest_file_path)
            print(f"The file {self.output_dest_file_path + self.output_file_name + '.csv'} has been deleted.")
        else:
            print(f"The file {self.output_dest_file_path + self.output_file_name + '.csv'} does not exist.")

        player_attributes_salaries_unification.player_attributes_salaries_unification(
            output_dest_file_path = self.output_dest_file_path,
            output_file_name = self.output_file_name
        )

        # gamelog_data_path: Path =' pipeline_output/gamelog/',
        # gamelog_name_pattern: str = 'gamelog',
        # schedule_data_path: Path =' pipeline_output/schedule/',
        # schedule_name_pattern: str = 'schedule',
        # unified_file_path: Path ='pipeline_output/final/',
        # unified_file_name: str = 'nba_games_training_dataset'

        assert os.path.exists(self.output_dest_file_path + self.output_file_name + '.csv') is True

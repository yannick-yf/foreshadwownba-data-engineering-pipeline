from unittest import TestCase
import os
from src.transform import gamelog_schedule_unification
import shutil

class TestFeatureSelection(TestCase):
    def setUp(self) -> None:
        self.unified_file_path = './tests/test_output/'
        self.unified_file_name = 'test'
        self.gamelog_data_path = 'pipeline_output/gamelog/'
        self.gamelog_name_pattern = 'gamelog'
        self.schedule_data_path = 'pipeline_output/schedule/'
        self.schedule_name_pattern = 'schedule'

    def test_gamelog_schedule_unification_w_args(self):

        # Check if the file exists before attempting to delete it
        if os.path.exists(self.unified_file_path + self.unified_file_name + '.csv'):
            shutil.rmtree(self.unified_file_path)
            print(f"The file {self.unified_file_path + self.unified_file_name + '.csv'} has been deleted.")
        else:
            print(f"The file {self.unified_file_path + self.unified_file_name + '.csv'} does not exist.")

        gamelog_schedule_unification.gamelog_schedule_unification(
                unified_file_path=self.unified_file_path,
                unified_file_name=self.unified_file_name,
                gamelog_data_path=self.gamelog_data_path,
                gamelog_name_pattern=self.gamelog_name_pattern,
                schedule_data_path=self.schedule_data_path,
                schedule_name_pattern=self.schedule_name_pattern
        )

        # gamelog_data_path: Path =' pipeline_output/gamelog/',
        # gamelog_name_pattern: str = 'gamelog',
        # schedule_data_path: Path =' pipeline_output/schedule/',
        # schedule_name_pattern: str = 'schedule',
        # unified_file_path: Path ='pipeline_output/final/',
        # unified_file_name: str = 'nba_games_training_dataset'

        assert os.path.exists(self.unified_file_path + self.unified_file_name + '.csv') is True

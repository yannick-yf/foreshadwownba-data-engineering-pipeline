from unittest import TestCase
import os
from src.transform import player_attributes_salaries_unification
import shutil

class TestPlayerAttributesSalariesUnification(TestCase):
    def setUp(self) -> None:
        self.output_dest_file_path = './tests/test_output/'
        self.output_file_name = 'player_attributes_salaries_dataset'
        self.players_attributes_data_path ='./tests/test_input/'
        self.players_attributes_name_pattern = 'player_attributes'
        self.players_salary_data_path ='./tests/test_input/'
        self.players_salary_name_pattern = 'player_salary'
        self.schedule_data_path = './tests/test_input/'
        self.schedule_name_pattern = 'schedule'

    def test_player_attributes_salaries_unification_w_args(self):

        # Check if the file exists before attempting to delete it
        if os.path.exists(self.output_dest_file_path + self.output_file_name + '.csv'):
            shutil.rmtree(self.output_dest_file_path)
            print(f"The file {self.output_dest_file_path + self.output_file_name + '.csv'} has been deleted.")
        else:
            print(f"The file {self.output_dest_file_path + self.output_file_name + '.csv'} does not exist.")

        player_attributes_salaries_unification.player_attributes_salaries_unification(
            schedule_data_path=self.schedule_data_path,
            schedule_name_pattern=self.schedule_name_pattern,
            player_attributes_data_path = self.players_attributes_data_path,
            player_attributes_name_pattern=self.players_attributes_name_pattern,
            player_salary_data_path=self.players_salary_data_path,
            player_salary_name_pattern=self.players_salary_name_pattern,
            output_dest_file_path = self.output_dest_file_path,
            output_file_name = self.output_file_name
        )

        assert os.path.exists(self.output_dest_file_path + self.output_file_name + '.csv') is True

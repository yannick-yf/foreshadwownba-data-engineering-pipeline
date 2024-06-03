from unittest import TestCase
import argparse
import pandas as pd
import os
from src.transform import gamelog_schedule_unification
import shutil
import glob

class TestFeatureSelection(TestCase):
    def setUp(self) -> None:
        self.data_path = './tests/test_output/'
        self.file_name = 'test'

    def test_gamelog_schedule_unification(self):

        # Check if the file exists before attempting to delete it
        if os.path.exists(self.data_path + self.file_name + '.csv'):
            shutil.rmtree(self.data_path)
            print(f"The file {self.data_path + self.file_name + '.csv'} has been deleted.")
        else:
            print(f"The file {self.data_path + self.file_name + '.csv'} does not exist.")

        gamelog_schedule_unification.gamelog_schedule_unification(
                data_path=self.data_path,
                file_name=self.file_name,
        )

        assert os.path.exists(self.data_path + self.file_name + '.csv') is True

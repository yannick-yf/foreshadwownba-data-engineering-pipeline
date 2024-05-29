from unittest import TestCase
import argparse
import pandas as pd

from src.stages import gamelog_data_acquisition


class TestFeatureSelection(TestCase):
    def setUp(self) -> None:
        self.season = 2020
        self.team=all
        self.data_type_gamelog='gamelog'
        self.data_type_schedule='schedule'

    def test_gamelog_data_acquisition(self):

        arg_parser = argparse.ArgumentParser()

        arg_parser.add_argument(
            '--config', 
            dest='config', 
            required=True)
        
        args = arg_parser.parse_args()
        
        gamelog_df = gamelog_data_acquisition.gamelog_data_acquisition(
            config_path = args.config
            )

        assert gamelog_df.shape[0] > 0
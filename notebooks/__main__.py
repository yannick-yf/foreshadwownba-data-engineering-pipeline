# __main__.py

# --------------------------------------------------------------------------------------------
# Aim of this script is to webscrap NBA games and schedules for multiple season and save it

# TODO: https://fastercapital.com/content/Pipeline-Validation--How-to-Validate-Your-Pipeline-Development-Data-and-Code-using-Pytest-and-DVC.html#Overview-of-Pytest-and-DVC
import sys

sys.path.append(".")

import pandas as pd
import datetime
from functions import (
    GetNbaGameDataCombinedCleaned,
    GetNbaPlayerDataCombinedCleanedProcessed,
)

season_to_pull = list(
    range(2022, 2024)
)  # 2022+ 1 to include the 2022 and/or coresponding to the current season usually

# TODO: Create a package for webscarppe
# TODO: Add log to this pipeline
# TODO: Use dvc for the data enginerring pipeline

if __name__ == "__main__":

    print("----------------------------------------------------------")
    print("Initialization")

    # ----------------------------------------------------------
    # Step 0: Init start time of the process
    start_time = datetime.datetime.now()

    # ------------------------------------------------------------------
    # Get NBA games for al the season requested by the user - Output of the process is saved in pipeline output
    print("Start - NBA Games data pull process")
    method_start_time = datetime.datetime.now()
    nba_games_multi_season_dataset = (
        GetNbaGameDataCombinedCleaned.fn_get_seasons_nba_games_data(season_to_pull)
    )
    method_end_time = datetime.datetime.now()
    duration = method_end_time - method_start_time
    print(f"NBA Games data pull process, duration: {duration}")
    print("-----------------")

    # ------------------------------------------------------------------
    # Schedule and Overtime games - Output of the process is saved in pipeline output
    print("Start - NBA Schedules & overtime data pull process")
    method_start_time = datetime.datetime.now()
    nba_schedules_multi_season_dataset = (
        GetNbaGameDataCombinedCleaned.fn_get_seasons_nba_schedules_overtime_data(
            season_to_pull
        )
    )
    method_end_time = datetime.datetime.now()
    duration = method_end_time - method_start_time
    print(f"NBA Schedules & overtime data pull process, duration: {duration}")
    print("-----------------")

    # ------------------------------------------------------------------
    # Cleaning And Consolidation Steps - Part of Engineering process
    print("Start - Cleaned & Combined process")
    method_start_time = datetime.datetime.now()
    training_dataset = GetNbaGameDataCombinedCleaned.cleaned_and_combined(
        nba_games_multi_season_dataset, nba_schedules_multi_season_dataset
    )
    method_end_time = datetime.datetime.now()
    duration = method_end_time - method_start_time
    print(f"Cleaned & Combined process, duration: {duration}")
    print("-----------------")

    # ------------------------------------------------------------------
    # Get NBA players list and attributes data for all the season requested by the user - Output of the process is saved in pipeline output
    print("Start - Players list and attributes data pull process")
    method_start_time = datetime.datetime.now()
    nba_list_players_multi_season_dataset = GetNbaPlayerDataCombinedCleanedProcessed.fn_get_nba_list_players_and_attributes_data(
        season_to_pull
    )
    method_end_time = datetime.datetime.now()
    duration = method_end_time - method_start_time
    print(f"NBA List players and attribute data pull process, duration: {duration}")
    print("-----------------")

    # ------------------------------------------------------------------
    # Get players list names processed to create id webscrappe
    print("Start - list players cleaning process")
    method_start_time = datetime.datetime.now()
    nba_id_webscrappe_players = (
        GetNbaPlayerDataCombinedCleanedProcessed.fn_get_id_webscrappe_for_players_games_data()
    )
    method_end_time = datetime.datetime.now()
    duration = method_end_time - method_start_time
    print(
        f"NBA list players cleaning and id webscrappe creation process, duration: {duration}"
    )
    print("-----------------")

    # ------------------------------------------------------------------
    # Get NBA players games data for all the season requested by the user - Output of the process is saved in pipeline output
    print("Start - NBA players boxscores games data pull process")
    method_start_time = datetime.datetime.now()
    nba_players_boxscores = (
        GetNbaPlayerDataCombinedCleanedProcessed.fn_get_players_boxscores_data()
    )
    method_end_time = datetime.datetime.now()
    duration = method_end_time - method_start_time
    print(f"NBA players boxscores pulling process, duration: {duration}")
    print("-----------------")

    # ------------------------------------------------------------------
    # Get NBA players Salary data for all the season requested by the user - Output of the process is saved in pipeline output
    print("Start - NBA players salary data pull process")
    method_start_time = datetime.datetime.now()
    nba_players_salaries = (
        GetNbaPlayerDataCombinedCleanedProcessed.fn_get_nba_players_salary_data(
            season_to_pull
        )
    )
    method_end_time = datetime.datetime.now()
    duration = method_end_time - method_start_time
    print(f"NBA players salary pulling process, duration: {duration}")
    print("-----------------")

    # ------------------------------------------------------------------
    # Get NBA players Salary and attributes Cleaned and Combined data for all the season requested by the user - Output of the process is saved in pipeline output
    print("Start - NBA players salary data pull process")
    method_start_time = datetime.datetime.now()
    nba_players_attributes_salaries = (
        GetNbaPlayerDataCombinedCleanedProcessed.fn_clean_and_combined_player_attributes_and_salary()
    )
    method_end_time = datetime.datetime.now()
    duration = method_end_time - method_start_time
    print(
        f"NBA players Salary and attributes Cleaned and Combined processing process, duration: {duration}"
    )
    print("-----------------")

    # ------------------------------------------------
    # Save the dataframe for the training package: foreshadwownba-ml (games and players)

    print("Start - Writting All the data to CSV process")
    method_start_time = datetime.datetime.now()
    nba_games_multi_season_dataset.to_csv(
        "./pipeline_output/nba_games_training_dataset_"
        + datetime.datetime.today().strftime("%Y-%m-%d")
        + ".csv",
        index=False,
    )
    nba_schedules_multi_season_dataset.to_csv(
        "./pipeline_output/schedules_training_dataset_"
        + datetime.datetime.today().strftime("%Y-%m-%d")
        + ".csv",
        index=False,
    )
    training_dataset.to_csv(
        "./pipeline_output/final_training_dataset_"
        + datetime.datetime.today().strftime("%Y-%m-%d")
        + ".csv",
        index=False,
    )
    nba_list_players_multi_season_dataset.to_csv(
        "./pipeline_output/nba_list_players_multi_season_dataset_"
        + datetime.datetime.today().strftime("%Y-%m-%d")
        + ".csv",
        index=False,
    )
    nba_id_webscrappe_players.to_csv(
        "./pipeline_output/nba_id_webscrappe_players_"
        + datetime.datetime.today().strftime("%Y-%m-%d")
        + ".csv",
        index=False,
    )
    nba_players_boxscores.to_csv(
        "./pipeline_output/nba_players_boxscores_"
        + datetime.datetime.today().strftime("%Y-%m-%d")
        + ".csv",
        index=False,
    )
    nba_players_salaries.to_csv(
        "./pipeline_output/nba_players_salaries_"
        + datetime.datetime.today().strftime("%Y-%m-%d")
        + ".csv",
        index=False,
    )
    nba_players_attributes_salaries.to_csv(
        "./pipeline_output/nba_players_attributes_salaries_"
        + datetime.datetime.today().strftime("%Y-%m-%d")
        + ".csv",
        index=False,
    )
    method_end_time = datetime.datetime.now()
    duration = method_end_time - method_start_time
    print(f"Writting data to CSV process, duration: {duration}")
    print("-----------------")

    # ------------------------------------------------
    # Last steps: Display diuration time
    end_time = datetime.datetime.now()
    duration = end_time - start_time
    print(f"Overall process, duration: {duration}")
    print("----------------------------------------------------------")

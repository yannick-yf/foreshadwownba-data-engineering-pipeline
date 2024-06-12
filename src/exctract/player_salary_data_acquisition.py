import pandas as pd
from typing import Text
import yaml
import argparse
import os

# WebScrapNbaSalary.py

from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import requests
import sys
import numpy as np
from urllib.error import HTTPError
import re
import lxml.html as lh
import time
from pathlib import Path
from src.utils.logs import get_logger

logger = get_logger(
    "PLAYER_SALARIES_DATA_ACQUISITION", log_level='INFO'
)


def player_salary_data_acquisition(
        data_type: str = 'player_salary',
        season: int = 2024,
        output_folder: Path = 'pipeline_output/player_salary/'
        ) -> None:
    """
    Gamelog data acquisition.
    Args:
        data_type (str): Argument from basketball_reference_webscrapper. Type of data to pull from the package
        season (int): Argument from basketball_reference_webscrapper. Season to pull from the package
        output_folder (Path): Path where to save the gamelog data pulled using the package.
    """

    # ------------------------------------------
    # Saving final training dataset

    player_salary_df = []
    url = "http://www.espn.com/nba/salaries/_/year/" + str(season) + "/seasontype/"
    data, page_total = scrape_page(url)
    player_salary_df.append(pd.DataFrame(data))

    # Get Salary Data
    for k in range(2, page_total + 1):
        url = (
            "http://www.espn.com/nba/salaries/_/year/" + str(season) + "/page/" + str(k)
        )
        Dict = scrape_page(url)[0]
        player_salary_df.append(pd.DataFrame(Dict))
        logger.info("Execution page number: %s", k)

        time.sleep(3)

    player_salary_df = pd.concat(player_salary_df)
    player_salary_df["year"] = int(season)
    player_salary_df = player_salary_df[player_salary_df["RK"] != "RK"]
    player_salary_df.reset_index(inplace=True, drop=True)

    # Convert salary to numeric
    for i in range(len(player_salary_df)):
        player_salary_df.loc[i, "name"] = re.sub(",.*", "", player_salary_df["NAME"][i])
        player_salary_df.loc[i, "salary"] = re.sub(
            "\$", "", player_salary_df["SALARY"][i]
        )
        player_salary_df.loc[i, "salary"] = re.sub(
            ",", "", player_salary_df["salary"][i]
        )

    # Get Rid of text rows
    player_salary_df["salary"] = pd.to_numeric(player_salary_df["salary"])

    #################################################################

    folder = output_folder
    isExist = os.path.exists(folder)
    if not isExist:
        os.makedirs(folder)

    name_and_path_file = (
        str(folder)
        + data_type
        + "_"
        + str(season)
        + ".csv"
    )

    player_salary_df.to_csv(name_and_path_file, index=False)

    logger.info("Player Salary Data Acquisition complete")


def scrape_page(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    }
    r = requests.get(url, headers=headers)

    # Get number of pages
    soup = BeautifulSoup(r.content, features="html.parser")

    page_total = soup.find(class_="page-numbers").get_text()
    page_total = re.sub(".*of", "", page_total).strip()
    page_total = int(page_total)
    # r = requests.get(url)
    doc = lh.fromstring(r.content)
    tr_elements = doc.xpath("//tr")
    # Create empty list
    col = []
    i = 0
    # For each row, store each first element (header) and an empty list
    for t in tr_elements[0]:
        i += 1
        name = t.text_content()
        col.append((name, []))
    for j in range(len(tr_elements)):
        # T is our j'th row
        T = tr_elements[j]
        # i is the index of our column
        i = 0
        # Iterate through each element of the row
        for t in T.iterchildren():
            data = t.text_content()
            # Check if row is empty
            if i > 0:
                # Convert any numerical value to integers
                try:
                    data = int(data)
                except:
                    pass
            # Append the data to the empty list of the i'th column
            col[i][1].append(data)
            # Increment i for the next column
            i += 1

    dict = {title: column for (title, column) in col}
    return dict, page_total



def get_args():
    """
    Parse command line arguments and return the parsed arguments.

    Returns:
        argparse.Namespace: Parsed command line arguments.
    """
    _dir = Path(__file__).parent.resolve()
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--params-file",
        type=Path,
        default="params.yaml",
    )

    args, _ = parser.parse_known_args()
    params = yaml.safe_load(args.params_file.open())

    player_salary_data_acquisition_params = params["player_salary_data_acquisition"]
    global_params = params['global_params']

    parser.add_argument(
        "--output-folder",
        dest="output_folder",
        type=Path,
        default=player_salary_data_acquisition_params["output_folder"],
    )

    parser.add_argument(
        "--data-type",
        dest="data_type",
        type=str,
        default=player_salary_data_acquisition_params["data_type"],
    )

    parser.add_argument(
        "--season",
        dest="season",
        type=int,
        default=global_params["season"],
    )

    args = parser.parse_args()

    return args

def main():
    """Run the Pre Train Multiple Models Pipeline."""
    args = get_args()

    player_salary_data_acquisition(
        data_type=args.data_type,
        season=args.season,
        output_folder=args.output_folder,
    )

if __name__ == "__main__":
    main()
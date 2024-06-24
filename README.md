```markdown
# Foreshadownba Data Engineering Pipeline

## Overview

The `foreshadownba-data-engineering-pipeline` is a data engineering project designed to extract, transform, and load NBA data. The pipeline is built using Python, DVC (Data Version Control) for getting the historical data, and Apache Airflow to automate the inseason workflows.

## Table of Contents

- [Installation](#installation)
- [Project Structure](#project-structure)
- [Pipeline Stages](#pipeline-stages)
  - [Data Acquisition](#data-acquisition)
  - [Data Transformation](#data-transformation)
  - [Data Loading](#data-loading)
- [Running the Pipeline](#running-the-pipeline)
- [Airflow DAGs](#airflow-dags)
- [Contributing](#contributing)
- [License](#license)

## Installation

Clone the repository:
```bash
git clone https://github.com/yourusername/foreshadownba-data-engineering-pipeline.git
cd foreshadownba-data-engineering-pipeline
```

To install the project, you'll need to have [Poetry](https://python-poetry.org/docs/#installation) installed. Then, you can set up the project environment with:

```bash
poetry install
```

Initiate DVC
```bash
dvc init
```

This will install all necessary dependencies listed in the `pyproject.toml` file.

## Project Structure

The project is organized as follows:

```plaintext
foreshadownba-data-engineering-pipeline/
│
├── dvc.yaml               # DVC pipeline stages
├── dags/                  # Airflow DAGs
│   └── inseason_dags.py
├── src/                   # Source code for data acquisition, transformation, and loading
│   ├── exctract/
│   │   ├── gamelog_data_acquisition.py
│   │   ├── schedule_data_acquisition.py
│   │   ├── player_attributes_data_acquisition.py
│   │   └── player_salary_data_acquisition.py
│   ├── transform/
│   │   ├── gamelog_schedule_unification.py
│   │   └── player_attributes_salaries_unification.py
│   └── load/
│       ├── load_gamelog_schedule_unified_to_db.py
│       └── load_player_attributes_salaries_unified_to_db.py
├── tests/                 # Unit tests
├── pyproject.toml         # Poetry configuration file
└── README.md              # Project readme
```

## Pipeline Stages

DVC Pipeline
The DVC pipeline is defined in the dvc.yaml file. It consists of the following stages:

1. gamelog_data_acquisition: Acquires game log data for each season.
2. schedule_data_acquisition: Acquires schedule data for each season.
3. player_attributes_data_acquisition: Acquires player attributes data for each season.
4. player_salary_data_acquisition: Acquires player salary data for each season.
5. gamelog_schedule_unification: Unifies game log and schedule data.
6. player_attributes_salaries_unification: Unifies player attributes and salary data.
7. load_gamelog_schedule_unified_to_db: Loads the unified game log and schedule data into the database.
8. load_player_attributes_salaries_unified_to_db: Loads the unified player attributes and salary data into the database.

### Data Acquisition

1. **Game Log Data Acquisition**:
    - Extracts game log data for each season.
    - Source file: `src/exctract/gamelog_data_acquisition.py`

2. **Schedule Data Acquisition**:
    - Extracts schedule data for each season.
    - Source file: `src/exctract/schedule_data_acquisition.py`

3. **Player Attributes Data Acquisition**:
    - Extracts player attributes data for each season.
    - Source file: `src/exctract/player_attributes_data_acquisition.py`

4. **Player Salary Data Acquisition**:
    - Extracts player salary data for each season.
    - Source file: `src/exctract/player_salary_data_acquisition.py`

### Data Transformation

1. **Game Log and Schedule Unification**:
    - Unifies game log and schedule data.
    - Source file: `src/transform/gamelog_schedule_unification.py`

2. **Player Attributes and Salaries Unification**:
    - Unifies player attributes and salary data.
    - Source file: `src/transform/player_attributes_salaries_unification.py`

### Data Loading

1. **Load Game Log and Schedule to Database**:
    - Loads the unified game log and schedule data into the database.
    - Source file: `src/load/load_gamelog_schedule_unified_to_db.py`

2. **Load Player Attributes and Salaries to Database**:
    - Loads the unified player attributes and salaries data into the database.
    - Source file: `src/load/load_player_attributes_salaries_unified_to_db.py`

## Running the Pipeline

You can reproduce the entire pipeline using DVC with:

```bash
dvc repro
```

This will execute all the stages defined in the `dvc.yaml` file.

## Airflow DAGs

The project includes an Airflow DAG for scheduling and running the pipeline:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from src.exctract import gamelog_data_acquisition, schedule_data_acquisition, player_attributes_data_acquisition, player_salary_data_acquisition
from src.transform import gamelog_schedule_unification, player_attributes_salaries_unification
from src.load import load_gamelog_schedule_unified_to_db, load_player_attributes_salaries_unified_to_db

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

season = 2024

with DAG('inseason_dags', default_args=default_args, schedule_interval='@daily') as dag:
    ...
```

To use the DAG, place it in the Airflow `dags/` directory and start the Airflow scheduler.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License

This project is licensed under the MIT License.
```

Feel free to modify the sections as needed to better fit your project's specifics.
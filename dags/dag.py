from datetime import datetime, timedelta
from airflow import DAG
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

with DAG(
    dag_id='inseason-data-integration',
    start_date=datetime.datetime(2024, 27, 6),
    schedule="@daily",
    ):
    gamelog_data_acquisition_task = PythonOperator(
        task_id=f'gamelog_data_acquisition_{season}',
        bash_command=gamelog_data_acquisition.gamelog_data_acquisition(
            season=season
        )
    )

    schedule_data_acquisition_task = PythonOperator(
        task_id=f'schedule_data_acquisition_{season}',
        bash_command=schedule_data_acquisition.schedule_data_acquisition(
            season=season
        )
    )

    player_attributes_data_acquisition_task = PythonOperator(
        task_id=f'player_attributes_data_acquisition_{season}',
        bash_command=player_attributes_data_acquisition.player_attributes_data_acquisition(
            season=season
        )
    )

    player_salary_data_acquisition_task = PythonOperator(
        task_id=f'player_salary_data_acquisition_{season}',
        python_callable=player_salary_data_acquisition.player_salary_data_acquisition(
            season=season
        )
    )

    gamelog_schedule_unification_task = PythonOperator(
        task_id=f'gamelog_schedule_unification_{season}',
        python_callable=gamelog_schedule_unification.gamelog_schedule_unification()
    )

    player_attributes_salaries_unification_task = PythonOperator(
        task_id=f'player_attributes_salaries_unification_{season}',
        python_callable=player_attributes_salaries_unification.player_attributes_salaries_unification()
    )

    load_gamelog_schedule_unified_to_db_task = PythonOperator(
        task_id=f'load_gamelog_schedule_unified_to_db_{season}',
        python_callable=load_gamelog_schedule_unified_to_db.load_gamelog_schedule_unified_to_db()
    )

    load_player_attributes_salaries_unified_to_db_task = PythonOperator(
        task_id=f'load_player_attributes_salaries_unified_to_db_{season}',
        python_callable=load_player_attributes_salaries_unified_to_db.load_player_attributes_salaries_unified_to_db()
    )

    # Set task dependencies
    gamelog_data_acquisition_task >> schedule_data_acquisition_task >> player_attributes_data_acquisition_task >> player_salary_data_acquisition_task

    gamelog_schedule_unification_task << [gamelog_data_acquisition_task, schedule_data_acquisition_task]
    player_attributes_salaries_unification_task << [player_attributes_data_acquisition_task, player_salary_data_acquisition_task, schedule_data_acquisition_task]

    gamelog_schedule_unification_task >> load_gamelog_schedule_unified_to_db_task
    player_attributes_salaries_unification_task >> load_player_attributes_salaries_unified_to_db_task

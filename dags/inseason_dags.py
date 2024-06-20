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

with DAG:
    gamelog_data_acquisition_task = BashOperator(
        task_id=f'gamelog_data_acquisition_{season}',
        bash_command=gamelog_data_acquisition.gamelog_data_acquisition(
            data_type='{{ var.value.gamelog_data_acquisition.data_type }}',
            season=season,
            output_folder='{{ var.value.gamelog_data_acquisition.output_folder }}'
        )
    )

    schedule_data_acquisition_task = BashOperator(
        task_id=f'schedule_data_acquisition_{season}',
        bash_command=schedule_data_acquisition.schedule_data_acquisition(
            season=season,
            output_folder='{{ var.value.schedule_data_acquisition.output_folder }}'
        )
    )

    player_attributes_data_acquisition_task = BashOperator(
        task_id=f'player_attributes_data_acquisition_{season}',
        bash_command=player_attributes_data_acquisition.player_attributes_data_acquisition(
            season=season,
            output_folder='{{ var.value.player_attributes_data_acquisition.output_folder }}'
        )
    )

    player_salary_data_acquisition_task = BashOperator(
        task_id=f'player_salary_data_acquisition_{season}',
        bash_command=player_salary_data_acquisition.player_salary_data_acquisition(
            season=season,
            output_folder='{{ var.value.player_salary_data_acquisition.output_folder }}'
        )
    )

    gamelog_schedule_unification_task = BashOperator(
        task_id='gamelog_schedule_unification',
        bash_command=gamelog_schedule_unification.gamelog_schedule_unification(
            gamelog_data_path='{{ var.value.project.directory }}/{{ var.value.gamelog_data_acquisition.output_folder }}',
            gamelog_name_pattern='{{ var.value.gamelog_data_acquisition.data_type }}',
            schedule_data_path='{{ var.value.project.directory }}/{{ var.value.schedule_data_acquisition.output_folder }}',
            schedule_name_pattern='{{ var.value.schedule_data_acquisition.data_type }}',
            unified_file_path='{{ var.value.project.directory }}/{{ var.value.gamelog_schedule_unification.data_path }}',
            unified_file_name='{{ var.value.gamelog_schedule_unification.file_name }}'
        )
    )

    player_attributes_salaries_unification_task = BashOperator(
        task_id='player_attributes_salaries_unification',
        bash_command=player_attributes_salaries_unification.player_attributes_salaries_unification(
            schedule_data_path='{{ var.value.project.directory }}/{{ var.value.schedule_data_acquisition.output_folder }}',
            schedule_name_pattern='{{ var.value.schedule_data_acquisition.data_type }}',
            player_attributes_data_path='{{ var.value.project.directory }}/{{ var.value.player_attributes_data_acquisition.output_folder }}',
            player_attributes_name_pattern='{{ var.value.player_attributes_data_acquisition.data_type }}',
            player_salary_data_path='{{ var.value.project.directory }}/{{ var.value.player_salary_data_acquisition.output_folder }}',
            player_salary_name_pattern='{{ var.value.player_salary_data_acquisition.data_type }}',
            output_dest_file_path='{{ var.value.project.directory }}/{{ var.value.player_attributes_salaries_unification.data_path }}',
            output_file_name='{{ var.value.player_attributes_salaries_unification.file_name }}'
        )
    )

    load_gamelog_schedule_unified_to_db_task = PythonOperator(
        task_id='load_gamelog_schedule_unified_to_db',
        python_callable=load_gamelog_schedule_unified_to_db.load_gamelog_schedule_unified_to_db
    )

    load_player_attributes_salaries_unified_to_db_task = PythonOperator(
        task_id='load_player_attributes_salaries_unified_to_db',
        python_callable=load_player_attributes_salaries_unified_to_db.load_player_attributes_salaries_unified_to_db
    )

    # Set task dependencies
    gamelog_data_acquisition_task >> schedule_data_acquisition_task >> player_attributes_data_acquisition_task >> player_salary_data_acquisition_task

    gamelog_schedule_unification_task << [gamelog_data_acquisition_task, schedule_data_acquisition_task]
    player_attributes_salaries_unification_task << [player_attributes_data_acquisition_task, player_salary_data_acquisition_task, schedule_data_acquisition_task]

    gamelog_schedule_unification_task >> load_gamelog_schedule_unified_to_db_task
    player_attributes_salaries_unification_task >> load_player_attributes_salaries_unified_to_db_task

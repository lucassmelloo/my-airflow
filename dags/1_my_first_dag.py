from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    '1_my_first_dag',
    start_date=datetime.today(),
    schedule_interval='@daily'
) as dag:
    
    task_1 = EmptyOperator(task_id='task_1')
    task_2 = EmptyOperator(task_id='task_2')
    task_3 = EmptyOperator(task_id='task_3')

    task_4 = BashOperator(
        task_id='create_dag_folder',
        bash_command = 'mkdir -p /opt/airflow/shared/1_my_first_dag'
    )
    task_5 = BashOperator(
        task_id='create_folder_task',
        bash_command = 'mkdir -p /opt/airflow/shared/1_my_first_dag/{{data_interval_end}}'
    )

    task_1 >> [task_2,task_3]
    task_3 >> task_4
    task_4 >> task_5

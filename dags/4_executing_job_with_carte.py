
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow_pentaho.operators.carte import CarteJobOperator

with DAG(
    dag_id='4_executing_job_with_carte',
    description='Trying to execute a Job using CarteJobOperator',
    start_date=days_ago(2),
    schedule_interval='@daily'
) as dag:

    tarefa_4 = BashOperator(
        task_id='create_dag_folder',
        bash_command = 'mkdir -p /opt/airflow/shared/primeiro_dag'
    )
    tarefa_5 = BashOperator(
        task_id='create_folder_task',
        bash_command = 'mkdir -p /opt/airflow/shared/primeiro_dag/{{data_interval_end}}'
    )

    tarefa_1 >> [tarefa_2,tarefa_3]
    tarefa_3 >> tarefa_4
    tarefa_4 >> tarefa_5

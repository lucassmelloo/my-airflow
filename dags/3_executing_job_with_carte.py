
from airflow.models import DAG
from airflow_pentaho.operators.carte import CarteJobOperator
from datetime import datetime,timedelta

with DAG(
    dag_id='4_executing_job_with_carte',
    description='Trying to execute a Job using CarteJobOperator',
    start_date= datetime.today()
) as dag:

    task = CarteJobOperator(
        pdi_conn_id='pentaho_server',
        task_id='executing_dag_with_airflow' ,
        job="C:\\\\Transformations\\\\Vendas_diarias\\\\Jobs\\\\Carga_VendasDiarias_e_Orcamento.kjb"
    )



    

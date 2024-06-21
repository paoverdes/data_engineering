from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from air_quality import create_connection_and_structure, process_air_quality_data, verify_information

default_args = {
    'owner': 'PaoVerdes',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    default_args=default_args,
    dag_id='DAG_LOAD_INFORMATION_WORKFLOW',
    description='DAG to request API and load data in Redshift',
    start_date=datetime(2024, 6, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    db_config = PythonOperator(task_id='create_connection_and_structure',
                               python_callable=create_connection_and_structure, dag=dag, provide_context=True)
    air_quality_data = PythonOperator(task_id='process_air_quality_data',
                               python_callable=process_air_quality_data, dag=dag, provide_context=True)
    send_email_if_anomaly = PythonOperator(task_id='send_email_if_anomaly',
                                python_callable=verify_information, dag=dag, provide_context=True)
    db_config >> air_quality_data >> send_email_if_anomaly

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(
    dag_id="mekari_dag",
    schedule_interval='@daily',
    start_date=datetime(year=2023, month=11, day=9),
    catchup=False
) as dag :
    # 1. employee csv to table
    task_get_employee_data = PythonOperator(
        task_id = 'get_employee_data',
        # python_callable= get_employee_data,
        do_xcom_push = True
    )
    # 2. timesheets csv to table
    # 3. employees dedup transformation
    # 4. employees & timesheets join and cleaning transformation
    # 5. final table aggregate monthly_branch_salary
    
from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator , PythonOperator


def simple_function(**context):
    ti = context["ti"]
    ti.xcom_push(key = "name" , value = "chetan")


def simple_fun2(**context):
    ti = context["ti"]
    name = ti.xcom_pull(task_ids = 'python_task_one' , key= 'name')

    print(f"And my name is {name}")
 


default_args = {
    "owner" : "Chetan",
    "retries": 0

}

with DAG(
    dag_id = "XCOM_DAGS",
    default_args=default_args,
    description = "description",
    start_date = datetime(2026, 8,12 ),
    catchup=True,
    schedule_interval = "@daily"
) as dag:
    
    python_task_one = PythonOperator(
        task_id = "python_task_one" , 
        python_callable = simple_function

    )

    python_task_three = PythonOperator(
        task_id = 'python_task_three',
        python_callable = simple_fun2
    )


    python_task_one >> python_task_three
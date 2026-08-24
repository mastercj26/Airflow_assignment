from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator





default_args = {
    "owner" : "Prasun",
    "retries": 0

}

with DAG(
    dag_id = "Our_first_DAG_1",
    default_args=default_args,
    description = "description",
    start_date = datetime(2026, 8,12 ),
    catchup=True,
    schedule_interval = "@daily"
) as dag:

    def function_null(name):
       print(f"Hello my name is : {name}")
       return name
    

    first_task = BashOperator(
        task_id = "first_task",
        bash_command = "echo hello world!"
    )

    second_task = BashOperator(
        task_id = "second_task" , 
        bash_command = "echo second task running as well"

    )
    third_task = BashOperator(
        task_id = "third_task" , 
        bash_command = "echo second task running as well"
     )
    fourth_task = BashOperator(
        task_id = "fourth_task" , 
        bash_command = "echo second task running as well"
      )
    fifth_task = BashOperator(
        task_id = "fifth_task" , 
        bash_command = "python /opt/airflow/pyscript/new_name.py"
        )
    
    sixth_task = PythonOperator(
        task_id="sixth_task",
        python_callable= function_null,
        op_kwargs= {"name":"Chetan Jain"}
    )
    
    first_task>>[second_task,third_task]
    second_task>>fourth_task
    fourth_task>>fifth_task>>sixth_task
import datetime

from airflow import DAG

from airflow.operators.python import PythonOperator

def first_function_to_execute():
    print('Hello World')
    return "Hello World"

with DAG("my_dag", start_date=datetime.datetime(2021,10,13), schedule_interval="@daily") as dag:
    training_model_A = PythonOperator(
        task_id = "training_model_a", 
        python_callable=first_function_to_execute
    )


# start_date = datetime.datetime.now()

# print(start_date)
# from datetime import timedelta
# from airflow import DAG
# #    from airflow.operators.python.operator import PythonOperator
# from datetime import datetime


try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from datetime import datetime
    print('All DAG Modules are OK')
except Exception as e:
    print('Error {} '.format(e))


# pip install "apache-airflow==2.1.4" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.1.4/constraints-3.9.txt"
# https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

def first_function_to_execute():
    print('Hello World')
    return "Hello World"


with DAG(
    dag_id = 'first_dag',
    schedule_interval='@daily',
    default_args = {
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'start_date': datetime.now()
    },
    catchup = False
) as f:
    first_function_to_execute = PythonOperator(
        task_id = "first_function_to_execute", 
        python_callable=first_function_to_execute
    )
base_log_folder = "/opt/airflow/logs/marketvol"


##running within the container

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from pathlib import Path
from datetime import date, datetime, timedelta


default_args = {
            "owner": "airflow",
            "start_date": datetime(2021, 9, 10),
            "retries": 2, # retry twice
            "retry_delay": timedelta(minutes=5) # five minutes interval
        }

def analyze_log_files():
    
    file_list = Path(base_log_folder).rglob('*.log')
    files = list(file_list)
    error_list = []
    for file in files:
        file_str = str(file)
        log_file = open(file_str,'r')
        for line in log_file:
            if "ERROR" in line:
                error_list.append(line)
    error_count = len(error_list)

    return error_count, error_list



with DAG(dag_id="log_analyzer",
         schedule_interval="6 0 * * 1-5", # running at 6pm for weekdays
         default_args=default_args,
         description='Analyze log data' ) as dag:

    task_0 = PythonOperator(
        task_id="get_log_data",
        python_callable= analyze_log_files,
        dag= dag
    )
    
   
#will show the logs of every subfolder within marketvol

task_0


#def log_analyzer(file):
    #function that outputs count of error entries as well as the relative errors
    #parsing function
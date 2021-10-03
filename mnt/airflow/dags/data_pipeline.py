"""
YOUR DATA PIPELINE GOES HERE
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import date, datetime, timedelta

import yfinance as yf
import pandas as pd

default_args = {
            "owner": "airflow",
            "start_date": datetime(2021, 3, 24),
            "depends_on_past": False,
            "email_on_failure": False,
            "retries": 2, # retry twice
            "retry_delay": timedelta(minutes=5) # five minutes interval
        }


def download_stock_data(stock):
    start_date = date.today()
    end_date = start_date + timedelta(days=1) 
    df = yf.download(stock, start=start_date, end=end_date, interval='1m')
    df.to_csv(stock + "_data.csv", header=True)




def get_last_stock_spread():
    apple_data = pd.read_csv("/tmp/data/AAPL_data.csv",header=True).sort_values(by = "date time", ascending = False)
    tesla_data = pd.read_csv("/tmp/data/TSLA_data.csv",header=True).sort_values(by = "date time", ascending = False)
    spread = [apple_data['high'][0] - apple_data['low'][0], tesla_data['high'][0] - tesla_data['low'][0]]
    return spread


with DAG(dag_id="marketvol",
         schedule_interval="6 0 * * 1-5", # running at 6pm for weekdays
         default_args=default_args,
         description='source Apple and Tesla data' ) as dag:

    task_0 = BashOperator(
        task_id="task_0",
        bash_command='''mkdir -p /tmp/data/''' + str(date.today()), #naming the folder with the current day
        dag= dag
    )

    #directory is created in container
    task_1 = PythonOperator(
        task_id = 'download_tsla_data',
        python_callable = download_stock_data,
        op_kwargs={'stock':'TSLA'},
        dag=dag
    )
    task_2 = PythonOperator(
        task_id = 'download_aapl_data',
        python_callable = download_stock_data,
        op_kwargs={'stock': 'AAPL'},
        dag=dag
    )
    task_3 = BashOperator(
        task_id='transfer_tsla_to_hdfs',
        bash_command= 'mv /opt/airflow/TSLA_data.csv /tmp/data/'+str(date.today()),
        dag=dag
          )
    task_4 = BashOperator(
        task_id='transfer_aapl_to_hdfs',
        bash_command= 'mv /opt/airflow/AAPL_data.csv /tmp/data/'+str(date.today()),
        dag=dag
        )
        # task_3 = BashOperator(
        #     task_id='transfer_tsla_to_hdfs'
        #     bash_command= '''scp -P 2222 root@sandbox-hdp.hortonworks.com:data/aapl_data /Users/macbookpro/Desktop//tmp/data/''' + str(date.today()+'''/TSLA_data.cs'''
        #     dag=dag
        # )
        # task_4 = BashOperator(
        #     task_id='transfer_aapl_to_hdfs'
        #     bash_command= '''scp -P 2222 root@sandbox-hdp.hortonworks.com:data/aapl_data /Users/macbookpro/Desktop//tmp/data/''' + str(date.today()+ '''/AAPL_data.cs'''
        #     dag=dag
        # )
    task_5 = PythonOperator(
        task_id = 'get_last_stock_spread',
        python_callable = get_last_stock_spread
     )
    
    
    
task_0 >> [task_1,task_2]
task_1 >> task_3 
task_2 >> task_4
[task_3,task_4] >> task_5
#task_0 >> [task_1,task_2] >> task_5

#[task_3,task_4]

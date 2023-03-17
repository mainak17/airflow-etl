import airflow
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
from transformation import *

def etl():
    movies_df = extract_movies_df()
    users_df = extract_users_df()
    transformed_df = transform_avg_ratings(movies_df,users_df)
    load_df_to_db(transformed_df)   

default_args ={
    'owner' : 'mainak',
    'start_date' : airflow.utils.dates.days_ago(1),
    'depends_on_past' : True,
    'email' : ['info@example.com'],
    'email_on_faliure' : True,
    'email_on_retry' : False,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=30)
}


dag = DAG(dag_id='etl_pipeline',
          default_args=default_args,
          schedule_interval="0 0 * * * ")
etl_task = PythonOperator(task_id="etl_task",
                          python_callable = etl,
                          dag=dag)
etl()
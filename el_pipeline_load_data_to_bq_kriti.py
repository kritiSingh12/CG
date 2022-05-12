from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow import models

DAG_NAME = 'el_pipeline_load_data_to_bq_kriti'
owner = models.Variable.get("owner")
bucket_path = models.Variable.get("bucket_path_out")
project_id = models.Variable.get("project_id")
bq_dataset_name = models.Variable.get("bq_dataset_name")
bq_table_name = 'bike_share_2015_02'

SCHEMA = 'TripId:STRING,TripDuration:STRING,StartStationId:STRING,StartTime:STRING,StartStationName:STRING,EndStationId:STRING,EndTime:STRING,EndStationName:STRING,BikeId:STRING,UserType:STRING'
filename='bike_share_2015_02.csv'

args = {'owner': owner, 'start_date': days_ago(1), 'schedule_interval': "0 14 * * *"}

with DAG(dag_id=DAG_NAME, default_args=args) as dag:

    start = DummyOperator(
        task_id='start'
    )

    export_sql_to_gcs_bucket = BashOperator(
        task_id='export_sql_to_gcs_bucket',
        bash_command=f'gcloud sql export csv --project=test-gcve-340601 k-mysql-batch-instance {bucket_path}{filename} --database=Bikeshare --query="select * from Bikeshare.bike_share_2015_02;"',
        dag=dag)

    load_csv_from_gcs_to_bq = BashOperator(
        task_id='load_csv_from_gcs_to_bq',
        bash_command=f'bq load --project_id={project_id} --null_marker=NULL --replace=true --source_format=CSV {bq_dataset_name}.{bq_table_name} {bucket_path}{filename} {SCHEMA}',
        dag=dag)

    end = DummyOperator(
        task_id='end'
    )

    start >> export_sql_to_gcs_bucket >> load_csv_from_gcs_to_bq >> end
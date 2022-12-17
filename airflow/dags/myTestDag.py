from datetime import datetime, timedelta
import pendulum
import pytz
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from datetime import datetime

from airflow import DAG
from airflow.decorators import task


local_tz = pendulum.timezone("Asia/Kolkata")
default_args = {
    'owner': 'sahaj',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 17, tzinfo=local_tz),
    'email': ['priyadarshan.patilmdh@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(default_args=default_args, dag_id="acmeFlix_reports", start_date=datetime(2022, 12, 17))

generate_report = SparkSubmitOperator(task_id='generate_report_task',
conn_id='spark_default',
application='/Users/priyadarshanp/myOpensourceContribution/airflow/de-bootcamp-assignment-s2/target/scala-2.12/de-bootcamp-assignment-s2-assembly-0.1.0-SNAPSHOT.jar',
java_class='SimpleApp',
executor_cores=4,
total_executor_cores='4',
executor_memory='3G',
driver_memory='1G',
name='AcmeFLixReportJob',
dag=dag
)

generate_report
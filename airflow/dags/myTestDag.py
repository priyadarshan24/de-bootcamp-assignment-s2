from datetime import datetime, timedelta
import pendulum
import pytz
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from jproperties import Properties
from airflow.models import Variable
from airflow.models.xcom import XCom


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

# Retrieve properties
toBeProcessedFolderLocation = Variable.get("toBeProcessedFolderLocation")
processedFolderLocation = Variable.get("processedFolderLocation")
reportFolderLocation = Variable.get("reportFolderLocation")
wareHouseLocation = Variable.get("wareHouseLocation")

print("Printing toBeProcessedFolderLocation::" + toBeProcessedFolderLocation)
print("Printing processedFolderLocation::" + processedFolderLocation)
print("Printing reportFolderLocation::" + reportFolderLocation)
print("Printing wareHouseLocation::" + wareHouseLocation)

argList = []
argList.append(toBeProcessedFolderLocation)
argList.append(processedFolderLocation)
argList.append(reportFolderLocation)
argList.append(wareHouseLocation)


validate_input_files = BashOperator(
    task_id="validate_input_files",
    bash_command="/Users/priyadarshanp/airflow/validateInputFileData.sh ",
    dag=dag,
    do_xcom_push=True
)


generate_report = SparkSubmitOperator(task_id='generate_report_task',
conn_id='spark_default',
application='/Users/priyadarshanp/myOpensourceContribution/airflow/de-bootcamp-assignment-s2/target/scala-2.12/de-bootcamp-assignment-s2-assembly-0.1.0-SNAPSHOT.jar',
java_class='SimpleApp',
executor_cores=4,
total_executor_cores='4',
executor_memory='3G',
driver_memory='1G',
name='AcmeFLixReportJob',
application_args=argList,
dag=dag
)

@task.branch(task_id="branch_task")
def branch_func(**kwargs):
    print("Inside branch_func::")
    ti = kwargs['ti']
    xcom_value = int(ti.xcom_pull(task_ids="validate_input_files"))
    print("Printing the xcom value::" + str(xcom_value))
    return "airflowDependencyTask"


@task()
def airflowDependencyTask():
    print("Printing that airflow task is been executed:::")

@task()
def sendErrorEmail():
    print("Sending error email notification:::")

branch_op = branch_func()

# Set dependencies between tasks
validate_input_files >> branch_op >> [generate_report,sendErrorEmail()] >> airflowDependencyTask()
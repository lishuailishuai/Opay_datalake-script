import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.configuration import conf
from datetime import datetime, timedelta

MAX_LOG_AGE_IN_DAYS = 7
BASE_LOG_FOLDER = conf.get("core", "BASE_LOG_FOLDER")

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime(2019, 7, 11),
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}
dag = airflow.DAG(
    'airflow-log-cleanup',
    schedule_interval="@weekly",
    default_args=args)

log_cleanup = """
echo "Getting Configurations..."
BASE_LOG_FOLDER='""" + BASE_LOG_FOLDER + """'
MAX_LOG_AGE_IN_DAYS='""" + str(MAX_LOG_AGE_IN_DAYS) + """'
echo "Finished Getting Configurations"
echo ""
echo "Configurations:"
echo "BASE_LOG_FOLDER:      '${BASE_LOG_FOLDER}'"
echo "MAX_LOG_AGE_IN_DAYS:  '${MAX_LOG_AGE_IN_DAYS}'"
echo ""

echo "Running Cleanup Process..."
FIND_DELETE_STATEMENT="find ${BASE_LOG_FOLDER}/*/* -type f -mtime +${MAX_LOG_AGE_IN_DAYS} -exec rm -f {} \;"
echo "Executing Find Delete Statement: ${FIND_DELETE_STATEMENT}"
eval ${FIND_DELETE_STATEMENT}
echo "Finished Running Cleanup Process"
"""

log_cleanup_t = BashOperator(
    task_id='log_cleanup_t',
    bash_command=log_cleanup,
    dag=dag)

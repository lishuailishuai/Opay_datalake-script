import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
import os

args = {
    'owner': 'root',
    'start_date': datetime(2019, 4, 20),
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    'import_opay_data',
    schedule_interval="30 01 * * *",
    default_args=args)

TABLES = [
    'business_users',
    'businesses',
    'chain_boxes',
    'chains',
    'fees',
    'financial_institutions',
    'merchants',
    'payment_order_data',
    'settlements',
    'terminal_owners',
    'terminals',
    'transactions',
]

def export_mongo_data(ds, **kwargs):
    file_name = "{table}/dt={dt}/{table}.json".format(dt=ds, table=kwargs['params']['table'])

    # export
    os.system('mongoexport --host 63.33.210.175 --port 27017 --db opay --collection {} --out /data/opay-mongo/{}'.format(kwargs['params']['table'], file_name))

    # gzip
    os.system('gzip /data/opay-mongo/{}'.format(file_name))

    # upload ucloud
    os.system('/root/filemgr/filemgr  --action mput --bucket opay-datalake --key opay_mongodb/{file}.gz --file /data/opay-mongo/{file}.gz'.format(file=file_name))

    # clear
    os.system('rm -f /data/opay-mongo/{}.gz'.format(file_name))

for table in TABLES:
    export_data = PythonOperator(
        task_id='export_data_{}'.format(table),
        python_callable=export_mongo_data,
        provide_context=True,
        params={'table':table},
        dag=dag
    )

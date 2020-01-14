import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
import logging
from airflow.models import Variable
import requests
import os


args = {
    'owner': 'root',
    'start_date': datetime(2019, 8, 30),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'import_appsflyer_report',
    schedule_interval="00 04 * * *",
    default_args=args)


def import_opay_install(ds, **kwargs):
    # download report
    api_url = "https://hq.appsflyer.com/export/team.opay.pay/installs_report/v5?api_token={api_token}&from={dt}&to={dt}&additional_fields=install_app_store,match_type,contributor1_match_type,contributor2_match_type,contributor3_match_type,device_category,gp_referrer,gp_click_time,gp_install_begin,amazon_aid,keyword_match_type".format(api_token=Variable.get("opay_appsflyer_api_token"), dt=ds)
    headers = {'Accept':'text/csv'}
    response = requests.get(
        api_url,
        headers=headers
    )
    logging.info('url:{} response_len:{}'.format(response.url, len(response.content)))
    tmp_path = '/tmp/'
    file_name = 'opay_appsflyer_opay_install_log_'+ds
    tmp_file = tmp_path + file_name
    with open(tmp_file, 'wb') as f:
        f.write(response.content)
    # upload to ufile
    upload_cmd = 'ossutil cp {tmp_file} oss://opay-datalake/opay/appsflyer/install_log/dt={dt}/{file_name} -u'.format(dt=ds, file_name=file_name,tmp_file=tmp_file)

    os.system(upload_cmd)
    # clear tmp file
    clear_cmd = 'rm -f %s' % tmp_file
    os.system(clear_cmd)

import_opay_install_log = PythonOperator(
    task_id='import_opay_install_log',
    python_callable=import_opay_install,
    provide_context=True,
    dag=dag
)

def import_opay_performance(ds, **kwargs):
    # download report
    api_url = "https://hq.appsflyer.com/export/team.opay.pay/geo_by_date_report/v5?api_token={api_token}&from={dt}&to={dt}".format(api_token=Variable.get("opay_appsflyer_api_token"), dt=ds)
    headers = {'Accept':'text/csv'}
    response = requests.get(
        api_url,
        headers=headers
    )
    logging.info('url:{} response_len:{}'.format(response.url, len(response.content)))
    tmp_path = '/tmp/'
    file_name = 'opay_appsflyer_opay_performance_log_'+ds
    tmp_file = tmp_path + file_name
    with open(tmp_file, 'wb') as f:
        f.write(response.content)
    # upload to ufile
    upload_cmd = 'ossutil cp {tmp_file} oss://opay-datalake/opay/appsflyer/performance_log/dt={dt}/{file_name} -u'.format(dt=ds, file_name=file_name,tmp_file=tmp_file)

    os.system(upload_cmd)
    # clear tmp file
    clear_cmd = 'rm -f %s' % tmp_file
    os.system(clear_cmd)

import_opay_performance_log = PythonOperator(
    task_id='import_opay_performance_log',
    python_callable=import_opay_performance,
    provide_context=True,
    dag=dag
)

add_dw_partitions = HiveOperator(
    task_id='add_dw_partitions',
    hql="""
            ALTER TABLE log_appsflyer_performance_di ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}');
            ALTER TABLE log_appsflyer_install_di ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}');

        """,
    schema='opay_dw',
    dag=dag)

import_opay_install_log >> add_dw_partitions
import_opay_performance_log >> add_dw_partitions
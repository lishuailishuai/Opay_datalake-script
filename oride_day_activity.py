# -*- coding: utf-8 -*-
import airflow
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from airflow.operators.bash_operator import BashOperator
from utils.connection_helper import get_hive_cursor
from airflow.models import Variable
import csv
import logging
import codecs

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime(2019, 9, 22),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_day_activity',
    schedule_interval="0 0,10,11,12,14,15,16 * * *",
    default_args=args
)

#send_mail
def send_report_email(ds, next_ds, next_execution_date, **kwargs):
    hour=next_execution_date.strftime("%H")
    if hour=='00':
        st=ds+" 00:00:00"
        et=ds+" 23:59:59"
    else:
        st=next_ds+" 00:00:00"
        et=next_ds+" %s:00:00" % hour
    sql="""
        SELECT
          o.driver_id,
          MAX(d.real_name) as real_name,
          MAX(d.phone_number) as phone_number,
          count(o.id) as completed_num,
          c.id,
          MAX(c.name) as city_name,
          dense_rank() over(PARTITION BY c.id ORDER BY count(o.id) desc) as rank
        FROM
          oride_bi.oride_day_activity_data_order o
          INNER JOIN oride_bi.oride_day_activity_data_driver d ON d.id=o.driver_id
          INNER JOIN oride_bi.oride_day_activity_data_driver_extend de ON de.id=o.driver_id
          INNER JOIN oride_bi.oride_day_activity_data_city_conf c ON c.id=de.city_id
        WHERE
          o.status in (4,5) and from_unixtime(o.create_time) BETWEEN '{st}' and '{et}'
          AND c.id<999001
        GROUP BY
          o.driver_id,c.id
        ORDER BY
          c.id,rank
    """.format(
        st=st,
        et=et
    )
    cursor = get_hive_cursor()
    logging.info('Executing: %s', sql)
    cursor.execute(sql)
    rows = cursor.fetchall()
    headers = [
        'driver_id',
        'real_name',
        'phone_number',
        'completed_num',
        'city_id',
        'city_name',
        'rank'
    ]
    file_name = '/tmp/oride_day_activity_{st}_{et}.csv'.format(st=st,et=et)
    with codecs.open(file_name, 'w', 'utf_8_sig') as f:
        f_csv = csv.writer(f)
        f_csv.writerow(headers)
        f_csv.writerows(rows)
    # send mail
    email_to = Variable.get("oride_day_activity_receivers").split()
    email_subject = 'ORide day 活动数据 {st}_{et}'.format(st=st,et=et)
    email_body = ''
    send_email(email_to, email_subject, email_body, [file_name], mime_charset='utf-8')

send_report = PythonOperator(
    task_id='send_report',
    python_callable=send_report_email,
    provide_context=True,
    dag=dag
)

#import db
table_list = [
    'data_order',
    'data_driver',
    'data_city_conf',
    'data_driver_extend',
]
conn_conf = BaseHook.get_connection("sqoop_db")
UFILE_PATH = 'ufile://opay-datalake/oride_day_activity/%s'
for table_name in table_list:
    import_table = BashOperator(
        task_id='import_table_{}'.format(table_name),
        bash_command='''
            #!/usr/bin/env bash
            sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
            -D mapred.job.queue.name=root.collects \
            --connect "jdbc:mysql://{host}:{port}/oride_data?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8" \
            --username {username} \
            --password {password} \
            --table {table} \
            --target-dir {ufile_path}/ \
            --fields-terminated-by "\\001" \
            --lines-terminated-by "\\n" \
            --hive-delims-replacement " " \
            --delete-target-dir \
            --compression-codec=snappy
        '''.format(
            host=conn_conf.host,
            port=conn_conf.port,
            username=conn_conf.login,
            password=conn_conf.password,
            table=table_name,
            ufile_path=UFILE_PATH % table_name
        ),
        dag=dag,
    )
    import_table >> send_report


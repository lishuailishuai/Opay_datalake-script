# coding: utf-8
import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from utils.connection_helper import get_hive_cursor
from airflow.utils.email import send_email
import xlwt
import logging
import codecs
from airflow.models import Variable
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from utils.validate_metrics_utils import *
from airflow.sensors import OssSensor

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2020, 2, 25),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'phone_car_order_bangka',
    schedule_interval="30 02 * * *",
    default_args=args)

table_names = ['ocredit_phones_dw_ods.ods_sqoop_base_t_order_df',
               'ocredit_carfinance_dw_ods.ods_sqoop_base_t_order_df',
               'opay_dw_ods.ods_sqoop_base_user_payment_instrument_df'
               ]

'''
校验分区代码
'''

phones_ods_sqoop_base_t_order_df_task = OssSensor(
    task_id='phones_ods_sqoop_base_t_order_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_phones_dw_sqoop/oloan/t_order",
        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

carfinance_ods_sqoop_base_t_order_df_task = OssSensor(
    task_id='carfinance_ods_sqoop_base_t_order_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="ocredit_carfinance_dw_sqoop/oloan_auto/t_order",
        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_user_payment_instrument_df_task = OssSensor(
    task_id='ods_sqoop_base_user_payment_instrument_df_task',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="opay_dw_sqoop/opay_user/user_payment_instrument",
        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

def send_phone_car_order_bangka_email(ds, **kwargs):
    cursor = get_hive_cursor()
    query_phone = '''
        SELECT from_unixtime(UNIX_TIMESTAMP(a.create_time)+3600,'yyyy-MM-dd HH:mm:ss') as create_time,
               cast(a.opay_id AS string) as opay_id,
               a.order_id as order_id,
               a.loan_time as loan_time,
               CASE
                   WHEN b.user_id IS NOT NULL THEN 1
                   ELSE 0
               END AS bangka
        FROM (select * 
        from ocredit_phones_dw_ods.ods_sqoop_base_t_order_df 
        where dt='{dt}' and order_status=81) a
        LEFT JOIN
          (SELECT user_id
           FROM opay_dw_ods.ods_sqoop_base_user_payment_instrument_df
           WHERE dt='{dt}'
             AND pay_status=1
           GROUP BY user_id) b ON cast(a.opay_id AS string)=b.user_id
    '''.format(dt=ds, ld=airflow.macros.ds_add(ds, -1), lw=airflow.macros.ds_add(ds, -7))

    query_car = '''
            SELECT from_unixtime(UNIX_TIMESTAMP(a.create_time)+3600,'yyyy-MM-dd HH:mm:ss') as create_time,
                   cast(a.opay_id AS string) as opay_id,
                   a.order_id as order_id,
                   a.pay_time as loan_time,
                   CASE
                       WHEN b.user_id IS NOT NULL THEN 1
                       ELSE 0
                   END AS bangka
            FROM (select * 
            from ocredit_carfinance_dw_ods.ods_sqoop_base_t_order_df
            where dt='{dt}' and order_status=61) a
            LEFT JOIN
              (SELECT user_id
               FROM opay_dw_ods.ods_sqoop_base_user_payment_instrument_df
               WHERE dt='{dt}'
                 AND pay_status=1
               GROUP BY user_id) b ON a.opay_id=b.user_id
        '''.format(dt=ds, ld=airflow.macros.ds_add(ds, -1), lw=airflow.macros.ds_add(ds, -7))

    logging.info('Executing: %s', query_phone)
    cursor.execute(query_phone)
    rows1 = cursor.fetchall()

    logging.info('Executing: %s', query_car)
    cursor.execute(query_car)
    rows2 = cursor.fetchall()

    file_name1 = '/tmp/phone_order_bangka_{dt}.xls'.format(dt=ds)
    file_name2 = '/tmp/car_order_bangka_{dt}.xls'.format(dt=ds)

    # 生成excel文件
    book1 = xlwt.Workbook()
    phone_order_bangka = book1.add_sheet('phone_order_bangka', cell_overwrite_ok=True)

    book2 = xlwt.Workbook()
    car_order_bangka = book2.add_sheet('car_order_bangka', cell_overwrite_ok=True)

    # 表头标题
    phone_order_bangka.write(0, 0, 'create_time')
    phone_order_bangka.write(0, 1, 'opay_id')
    phone_order_bangka.write(0, 2, 'order_id')
    phone_order_bangka.write(0, 3, 'loan_time')
    phone_order_bangka.write(0, 4, 'bangka')

    # 表头标题
    car_order_bangka.write(0, 0, 'create_time')
    car_order_bangka.write(0, 1, 'opay_id')
    car_order_bangka.write(0, 2, 'order_id')
    car_order_bangka.write(0, 3, 'loan_time')
    car_order_bangka.write(0, 4, 'bangka')

    # 每一列写入excel文件，不然数据会全在一个单元格中
    for i in range(len(rows1)):
        for j in range(5):
            # print (rows[i][j])-
            # print ("--------")
            phone_order_bangka.write(i + 1, j, rows1[i][j])

    # 每一列写入excel文件，不然数据会全在一个单元格中
    for i in range(len(rows2)):
        for j in range(5):
            # print (rows2[i][j])-
            # print ("--------")
            car_order_bangka.write(i + 1, j, rows2[i][j])

    book1.save(file_name1)
    book2.save(file_name2)
    cursor.close()

    # send mail

    email_to = Variable.get("phone_car_order_bangka").split()
    #email_to = ['lili.chen@opay-inc.com']
    result = is_alert(ds, table_names)
    if result:
        email_to = ['bigdata@opay-inc.com']

    email_subject = 'ocredit手机汽车业务绑卡数据{dt}'.format(dt=ds)
    email_body = ''
    send_email(email_to, email_subject, email_body, [file_name1], mime_charset='utf-8')


phone_car_order_bangka_email = PythonOperator(
    task_id='phone_car_order_bangka_email',
    python_callable=send_phone_car_order_bangka_email,
    provide_context=True,
    dag=dag
)

phones_ods_sqoop_base_t_order_df_task >> phones_ods_sqoop_base_t_order_df_task >> ods_sqoop_base_user_payment_instrument_df_task >> phone_car_order_bangka_email
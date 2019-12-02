# coding=utf-8
import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from airflow.utils.email import send_email
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.hive_hooks import HiveCliHook
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors import UFileSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'liushuzhen',
    'start_date': datetime(2019, 11, 19),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_opay_owealth_collect_d',
    schedule_interval="00 19 * * *",
    default_args=args)

##----------------------------------------- 依赖 ---------------------------------------##
ods_sqoop_owealth_share_acct_hf_prev_day_task = UFileSensor(
    task_id='ods_sqoop_owealth_share_acct_hf_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/hour=18/_SUCCESS'.format(
        hdfs_path_str="opay_owealth_sqoop_hf/opay_owealth/share_acct",
        pt='{{macros.ds_add(ds, +1)}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_owealth_share_order_hf_prev_day_task = UFileSensor(
    task_id='ods_sqoop_owealth_share_order_hf_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/hour=18/_SUCCESS'.format(
        hdfs_path_str="opay_owealth_sqoop_hf/opay_owealth/share_order",
        pt='{{macros.ds_add(ds, +1)}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw"
table_name = "app_opay_owealth_collect_d"
hdfs_path = "ufile://opay-datalake/opay/opay_dw/" + table_name


def app_opay_owealth_collect_d_sql_task(ds):
    HQL = '''

                    WITH acct_base AS
              (SELECT user_id,
                      create_time,
                      balance
               FROM opay_owealth_ods.ods_sqoop_owealth_share_acct_hf
               WHERE dt='{pt}' and hour='18'
               and from_unixtime(unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss')+3600)<'{pt} 19:00:00'
               
              ),
                 order_base AS
              (SELECT create_time,
                      order_type,
                      trans_amount,
                      user_id
               FROM opay_owealth_ods.ods_sqoop_owealth_share_order_hf
               WHERE dt='{pt}' and hour='18'
                 AND status="S"
                 AND from_unixtime(unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss')+3600)>='{yesterday} 18:00:00'
                 AND from_unixtime(unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss')+3600)<'{pt} 19:00:00'
              )
            INSERT overwrite TABLE opay_dw.app_opay_owealth_collect_d partition (dt='{pt}')
            SELECT total_balance, --总的累计金额
             total_subscribe_amount,--总的手动申购金额
             total_redeem_amount--总的赎回金额
            
            FROM
              (SELECT '{pt}' AS dt,
                      sum(balance) total_balance
               FROM acct_base a) m
            LEFT JOIN
              (SELECT '{pt}' AS dt,
                      sum(CASE
                              WHEN order_type='1001' THEN trans_amount
                          END) total_subscribe_amount,
                      sum(CASE
                              WHEN order_type='1002' THEN trans_amount
                          END) total_redeem_amount
               FROM order_base a)m1 ON m.dt=m1.dt



    '''.format(
        pt=airflow.macros.ds_add(ds, +1),
        yesterday=ds,
        db=db_name,
        table=table_name
    )

    return HQL


def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_opay_owealth_collect_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "false", "true")


app_opay_owealth_collect_d_task = PythonOperator(
    task_id='app_opay_owealth_collect_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)


def send_owealth_report_email(ds, **kwargs):
    sql = '''
        SELECT
           dt,
           total_balance/100,
           total_subscribe_amount/100,
           total_redeem_amount/100
           
        FROM
           opay_dw.app_opay_owealth_collect_d
        WHERE
            dt <= '{dt}'
        ORDER BY dt DESC
        limit 14
    '''.format(dt=ds)

    cursor = get_hive_cursor()
    logging.info(sql)
    cursor.execute(sql)
    data_list = cursor.fetchall()

    html_fmt = '''
            <html>
            <head>
            <title></title>
            <style type="text/css">
                table
                {{
                    font-family:'黑体';
                    border-collapse: collapse;
                    margin: 0 auto;
                    text-align: left;
                    font-size:12px;
                    color:#29303A;
                }}
                table h2
                {{
                    font-size:20px;
                    color:#000000;
                }}
                .th_title
                {{
                    font-size:16px;
                    color:#000000;
                    text-align: center;
                }}
                table td, table th
                {{
                    border: 1px solid #000000;
                    color: #000000;
                    height: 30px;
                    padding: 5px 10px 5px 5px;
                }}
                table thead th
                {{
                    background-color: #FFCC94;
                    //color: white;
                    width: 100px;
                }}
            </style>
            </head>
            <body>
                <table width="95%" class="table">
                    <caption>
                        <h2>OWealth日报</h2>
                    </caption>
                </table>


                <table width="95%" class="table">
                    <thead>
                     
                        <tr>
                            <th>日期</th>

                            <th>累计总额</th>
                            <th>总申购金额</th>
                            <th>赎回金额</th>

                        </tr>
                    </thead>
                    {rows}
                </table>

            </body>
            </html>
            '''

    row_html = ''

    if len(data_list) > 0:

        tr_fmt = '''
            <tr style="background-color:#F5F5F5;">{row}</tr>
        '''
        weekend_tr_fmt = '''
            <tr style="background:#F5F5F5">{row}</tr>
        '''
        row_fmt = '''
                 <td>{0}</td>
                <td>{1}</td>
                <td>{2}</td>
                <td>{3}</td>

        '''

        for data in data_list:
            row = row_fmt.format(*list(data))
            week = data[1]
            if week == '6' or week == '7':
                row_html += weekend_tr_fmt.format(row=row)
            else:
                row_html += tr_fmt.format(row=row)

    html = html_fmt.format(rows=row_html)
    # send mail

    #email_to = Variable.get("owealth_report_receivers").split()
    email_to = ['shuzhen.liu@opay-inc.com']

    email_subject = 'OWealth日报_{}'.format(ds)
    send_email(
        email_to
        , email_subject, html, mime_charset='utf-8')
    cursor.close()
    return


send_owealth_report = PythonOperator(
    task_id='send_owealth_report',
    python_callable=send_owealth_report_email,
    provide_context=True,
    dag=dag
)
ods_sqoop_owealth_share_acct_hf_prev_day_task >> app_opay_owealth_collect_d_task
ods_sqoop_owealth_share_order_hf_prev_day_task >> app_opay_owealth_collect_d_task
app_opay_owealth_collect_d_task >> send_owealth_report



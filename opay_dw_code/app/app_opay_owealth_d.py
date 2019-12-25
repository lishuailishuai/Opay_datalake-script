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
from airflow.sensors import OssSensor

import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'liushuzhen',
    'start_date': datetime(2019, 12, 20),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_opay_owealth_d',
    schedule_interval="50 03 * * *",
    default_args=args)

##----------------------------------------- 依赖 ---------------------------------------##
ods_sqoop_owealth_share_acct_df_prev_day_task = OssSensor(
    task_id='ods_sqoop_owealth_share_acct_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_owealth_ods/opay_owealth/share_acct",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_owealth_share_order_df_prev_day_task = OssSensor(
    task_id='ods_sqoop_owealth_share_order_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_owealth_ods/opay_owealth/share_order",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_user_di_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_user_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_di/opay_user/user",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw"
table_name = "app_opay_owealth_d"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def app_opay_owealth_d_sql_task(ds):
    HQL = '''
SET mapreduce.job.queuename= opay_collects;
set mapred.max.split.size=1000000;
        WITH acct_base AS
          (SELECT user_id,
                  create_time,
                  balance
           FROM opay_owealth_ods.ods_sqoop_owealth_share_acct_df
           WHERE dt='{pt}' and 
           substr(from_unixtime(unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss')+3600),1,10)<='{pt}'
          ),
             order_base AS
          (SELECT create_time,
                  order_type,
                  trans_amount,
                  user_id
           FROM opay_owealth_ods.ods_sqoop_owealth_share_order_df
           WHERE dt='{pt}'
             AND status="S"
             AND substr(from_unixtime(unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss')+3600),1,10)='{pt}'),
             user_base AS
          (
           select 
                user_id, ROLE, mobile
            from (
                select 
                    user_id, ROLE, mobile,
                    row_number() over(partition by user_id order by update_time desc) rn
                from opay_dw_ods.ods_sqoop_base_user_di
                where dt <= '{pt}'
            ) t1 where rn = 1
          )
        INSERT overwrite TABLE opay_dw.app_opay_owealth_d partition (dt='{pt}')
        SELECT   agent_balance+customer_balance total_balance, --总的累计金额
                 agent_subscribe_amount+customer_subscribe_amount total_subscribe_amount,--总的手动申购金额
                 agent_redeem_amount+customer_redeem_amount total_redeem_amount,--总的赎回金额
                 customer_balance,--customer累计金额
                 customer_subscribe_amount, --customer手动申购金额
                 customer_redeem_amount,--customer赎回金额
                 agent_balance,--agent累计金额
                 agent_subscribe_amount,--agent手动申购金额
                 agent_redeem_amount--agent赎回金额

        FROM
          (SELECT '{pt}' AS dt,
                  sum(CASE
                          WHEN ROLE='agent' THEN balance
                      END) agent_balance,
                  sum(CASE
                          WHEN ROLE='customer' THEN balance
                      END) customer_balance
           FROM acct_base a
           INNER JOIN user_base b ON a.user_id=b.mobile) m
        LEFT JOIN
          (SELECT '{pt}' AS dt,
                  sum(CASE
                          WHEN order_type='1001'
                               AND ROLE='agent' THEN trans_amount
                      END) agent_subscribe_amount,
                  sum(CASE
                          WHEN order_type='1001'
                               AND ROLE='customer' THEN trans_amount
                      END) customer_subscribe_amount,
                  sum(CASE
                          WHEN order_type='1002'
                               AND ROLE='agent' THEN trans_amount
                      END) agent_redeem_amount,
                  sum(CASE
                          WHEN order_type='1002'
                               AND ROLE='customer' THEN trans_amount
                      END) customer_redeem_amount
           FROM order_base a
           INNER JOIN user_base b ON a.user_id=b.mobile)m1 ON m.dt=m1.dt



    '''.format(
        pt=ds,
        db=db_name,
        table=table_name
    )

    return HQL


def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_opay_owealth_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "false", "true")


app_opay_owealth_d_task = PythonOperator(
    task_id='app_opay_owealth_d_task',
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
           total_redeem_amount/100,
           customer_balance/100,
           customer_subscribe_amount/100,
           customer_redeem_amount/100,
           agent_balance/100,
           agent_subscribe_amount/100,
           agent_redeem_amount/100
        FROM
           opay_dw.app_opay_owealth_d
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
                            <th></th>
                            <th colspan="3" class="th_title">汇总数据</th>
                            <th colspan="3" class="th_title">customer数据</th>
                            <th colspan="3" class="th_title">agent数据</th>
                        </tr>
                        <tr>
                            <th>日期</th>
                            <!--汇总数据-->

                            <th>累计总额</th>
                            <th>总申购金额</th>
                            <th>赎回金额</th>

                            <!--customer数据-->
                            <th>累计总额</th>
                            <th>总申购金额</th>
                            <th>赎回金额</th>

                            <!--agent数据-->
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
                <td>{4}</td>
                <td>{5}</td>
                <td>{6}</td>
                <td>{7}</td>
                <td>{8}</td>
                <td>{9}</td>

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

    email_to = Variable.get("owealth_report_receivers").split()

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
ods_sqoop_owealth_share_acct_df_prev_day_task >> app_opay_owealth_d_task
ods_sqoop_owealth_share_order_df_prev_day_task >> app_opay_owealth_d_task
ods_sqoop_base_user_di_prev_day_task >> app_opay_owealth_d_task
app_opay_owealth_d_task >> send_owealth_report



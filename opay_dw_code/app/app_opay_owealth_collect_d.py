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
    'start_date': datetime(2019, 12, 5),
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

ods_sqoop_base_owealth_user_subscribed_hf_prev_day_task = UFileSensor(
    task_id='ods_sqoop_base_owealth_user_subscribed_hf_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/hour=19/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_hf/opay_owealth/owealth_user_subscribed",
        pt='{{macros.ds_add(ds, +1)}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_owealth_share_revenue_log_hf_prev_day_task = UFileSensor(
    task_id='ods_sqoop_owealth_share_revenue_log_hf_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/hour=18/_SUCCESS'.format(
        hdfs_path_str="opay_owealth_sqoop_hf/opay_owealth/share_revenue_log",
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
    HQL = '''WITH acct_base AS
  (SELECT user_id,
          create_time,
          balance
   FROM opay_owealth_ods.ods_sqoop_owealth_share_acct_hf
   WHERE dt='{pt}'
     AND hour='18'
     AND from_unixtime(unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss')+3600)<'{pt} 19:00:00' ),
     order_base AS
  (SELECT create_time,
          order_type,
          trans_amount,
          user_id,
          memo
   FROM opay_owealth_ods.ods_sqoop_owealth_share_order_hf
   WHERE dt='{pt}'
     AND hour='18'
     AND status="S"
     AND from_unixtime(unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss')+3600)>='{yesterday} 19:00:00'
     AND from_unixtime(unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss')+3600)<'{pt} 19:00:00' ),
     user_subscribed AS
  (SELECT user_id,
          update_time,
          subscribed
   FROM opay_dw_ods.ods_sqoop_base_owealth_user_subscribed_hf
   WHERE dt='{pt}'
     AND hour='19'
     AND from_unixtime(unix_timestamp(update_time, 'yyyy-MM-dd HH:mm:ss')+3600)<'{pt} 19:00:00' ),
     revenue AS
  (SELECT *
   FROM opay_owealth_ods.ods_sqoop_owealth_share_revenue_log_hf
   WHERE dt='{pt}'
     AND hour='18'
     AND from_unixtime(unix_timestamp(revenue_date, 'yyyy-MM-dd HH:mm:ss')+3600)>='{yesterday} 19:00:00' 
    AND from_unixtime(unix_timestamp(revenue_date, 'yyyy-MM-dd HH:mm:ss')+3600)<'{pt} 19:00:00' ) 
INSERT overwrite TABLE opay_dw.app_opay_owealth_collect_d partition (dt='{pt}')

SELECT total_balance, --总的累计金额
 total_subscribe_amount,--总的申购金额
 total_redeem_amount,--总的赎回金额
 no_api_subscribe_amount,--总的手动申购金额
 api_subscribe_amount,--总的自动申购金额
 no_api_subscribe_user,--手动申购交易用户数
 api_subscribe_user,--自动申购交易用户数
 redeem_user,--赎回交易用户数
 add_open_api_subscribe_user,--累计开通自动申购用户数
 open_api_subscribe_user,--当天开通自动申购用户数
 close_api_subscribe_user,--当天关闭自用申购用户数
 revenue_amount --入账利息

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
                  WHEN order_type='1001'
                       AND memo='申购' THEN trans_amount
              END) no_api_subscribe_amount,
          sum(CASE
                  WHEN order_type='1001'
                       AND memo='API' THEN trans_amount
              END) api_subscribe_amount,
          count(DISTINCT CASE
                             WHEN memo='申购' THEN user_id
                         END) no_api_subscribe_user,
          count(DISTINCT CASE
                             WHEN memo='API' THEN user_id
                         END) api_subscribe_user,
          sum(CASE
                  WHEN order_type='1002' THEN trans_amount
              END) total_redeem_amount,
          count(DISTINCT CASE
                             WHEN memo='赎回' THEN user_id
                         END) redeem_user
   FROM order_base a)m1 ON m.dt=m1.dt
LEFT JOIN
  (SELECT '{pt}' AS dt,
          count(DISTINCT user_id) add_open_api_subscribe_user
   FROM user_subscribed) m2 ON m.dt=m2.dt
LEFT JOIN
  (SELECT '{pt}' AS dt,
          count(DISTINCT CASE
                             WHEN subscribed='Y' THEN user_id
                         END)open_api_subscribe_user,
          count(DISTINCT CASE
                             WHEN subscribed='N' THEN user_id
                         END) close_api_subscribe_user
   FROM user_subscribed
   WHERE from_unixtime(unix_timestamp(update_time, 'yyyy-MM-dd HH:mm:ss')+3600)>='{yesterday} 19:00:00' ) m3 ON m.dt=m3.dt
LEFT JOIN
  (SELECT '{pt}' AS dt,
          sum(revenue_amount) revenue_amount
   FROM revenue) m4 ON m.dt=m4.dt
   
   
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
    TaskTouchzSuccess().countries_touchz_success(airflow.macros.ds_add(ds, +1), db_name, table_name, hdfs_path, "false", "true")


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
           format_number(total_balance/100,2),
           format_number(total_subscribe_amount/100,2),
           format_number(total_redeem_amount/100,2),
           format_number(no_api_subscribe_amount/100,2),
           format_number(api_subscribe_amount/100,2),
           format_number(no_api_subscribe_user,0),
           format_number(api_subscribe_user,0),
           format_number(redeem_user,0),
           format_number(add_open_api_subscribe_user,0),
           format_number(open_api_subscribe_user,0),
           format_number(close_api_subscribe_user,0),
           format_number(revenue_amount/100,2)
           
        FROM
           opay_dw.app_opay_owealth_collect_d
        WHERE
            dt <= '{dt}'
        ORDER BY dt DESC
        limit 14
    '''.format(dt=airflow.macros.ds_add(ds, +1))

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
                        <h2>OWealth日报(19:00-19:00)</h2>
                    </caption>
                </table>


                <table width="95%" class="table">
                    <thead>
                     
                        <tr>
                            <th>日期</th>

                            <th>累计总额</th>
                            <th>总申购金额</th>
                            <th>总赎回金额</th>
                            <th>总的手动申购金额</th>
                            <th>总的自动申购金额</th>
                            <th>手动申购交易用户数</th>
                            <th>自动申购交易用户数</th>
                            <th>赎回交易用户数</th>
                            <th>累计开通自动申购用户数</th>
                            <th>当天开通自动申购用户数</th>
                            <th>当天关闭自用申购用户数</th>
                            <th>入账利息</th>

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
                 <td align="left">{0}</td>
                <td align="right">{1}</td>
                <td align="right">{2}</td>
                <td align="right">{3}</td>
                <td align="right">{4}</td>
                <td align="right">{5}</td>
                <td align="right">{6}</td>
                <td align="right">{7}</td>
                <td align="right">{8}</td>
                <td align="right">{9}</td>
                <td align="right">{10}</td>
                <td align="right">{11}</td>
                <td align="right">{12}</td>

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

    email_to = Variable.get("owealth_report_receivers_19").split()
    #email_to = ['shuzhen.liu@opay-inc.com']

    email_subject = 'OWealth日报_{}'.format(airflow.macros.ds_add(ds, +1))
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
ods_sqoop_base_owealth_user_subscribed_hf_prev_day_task >> app_opay_owealth_collect_d_task
ods_sqoop_owealth_share_revenue_log_hf_prev_day_task >> app_opay_owealth_collect_d_task
app_opay_owealth_collect_d_task >> send_owealth_report



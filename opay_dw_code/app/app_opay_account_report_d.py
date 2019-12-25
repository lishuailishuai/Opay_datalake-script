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
    'app_opay_account_report_d',
    schedule_interval="50 03 * * *",
    default_args=args)

##----------------------------------------- 依赖 ---------------------------------------##
dwm_opay_account_df_prev_day_task = OssSensor(
    task_id='dwm_opay_account_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwm_opay_account_df/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw"
table_name = "app_opay_account_report_d"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def app_opay_account_report_d_sql_task(ds):
    HQL = '''
    SET mapreduce.job.queuename= opay_collects;
    set mapred.max.split.size=1000000;
    insert overwrite table {db}.{table} partition (dt='{pt}')
    SELECT customer_bonus+customer_frozen+customer_cash+agent_bonus+agent_frozen+agent_cash+merchant_bonus+merchant_frozen+merchant_cash AS total,
       customer_bonus+agent_bonus+merchant_bonus AS bonus,
       customer_cash+agent_cash+merchant_cash AS cash,
       customer_frozen+agent_frozen+merchant_frozen AS frozen,
       customer_bonus+customer_frozen+customer_cash AS customer,
       customer_bonus,
       customer_cash,
       customer_frozen,
       agent_bonus+agent_frozen+agent_cash AS agent,
       agent_bonus,
       agent_cash,
       agent_frozen,
       merchant_bonus+merchant_frozen+merchant_cash AS merchant,
       merchant_bonus,
       merchant_cash,
       merchant_frozen
    FROM
      (SELECT dt,
              sum(CASE
                      WHEN user_role='customer'
                           AND account_type='BONUSACCOUNT' THEN balance
                  END) customer_bonus,
              sum(CASE
                      WHEN user_role='customer'
                           AND account_type='FROZENACCOUNT' THEN balance
                  END) customer_frozen,
              sum(CASE
                      WHEN user_role='customer'
                           AND account_type='CASHACCOUNT' THEN balance
                  END) customer_cash,
              sum(CASE
                      WHEN user_role='agent'
                           AND account_type='BONUSACCOUNT' THEN balance
                  END) agent_bonus,
              sum(CASE
                      WHEN user_role='agent'
                           AND account_type='FROZENACCOUNT' THEN balance
                  END) agent_frozen,
              sum(CASE
                      WHEN user_role='agent'
                           AND account_type='CASHACCOUNT' THEN balance
                  END) agent_cash,
              sum(CASE
                      WHEN user_role='merchant'
                           AND account_type='BONUSACCOUNT' THEN balance
                  END) merchant_bonus,
              sum(CASE
                      WHEN user_role='merchant'
                           AND account_type='FROZENACCOUNT' THEN balance
                  END) merchant_frozen,
              sum(CASE
                      WHEN user_role='merchant'
                           AND account_type='CASHACCOUNT' THEN balance
                  END) merchant_cash
       FROM opay_dw.dwm_opay_account_df
       WHERE dt='{pt}'
       GROUP BY dt)m

    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_opay_account_report_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "false", "true")


app_opay_account_report_d_task = PythonOperator(
    task_id='app_opay_account_report_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)
##-------------------------- 邮件发送 ----------------------------------##

def send_balance_report_email(ds, **kwargs):
    sql = '''
        SELECT
            dt,
            format_number(total/100,2),
            format_number(bonus/100,2),
            format_number(cash/100,2),
            format_number(frozen/100,2),
            format_number(customer/100,2),
            format_number(customer_bouns/100,2),
            format_number(customer_cash/100,2),
            format_number(customer_frozen/100,2),
            format_number(agent/100,2),
            format_number(agent_bouns/100,2),
            format_number(agent_cash/100,2),
            format_number(agent_frozen/100,2),
            format_number(merchant/100,2),
            format_number(merchant_bouns/100,2),
            format_number(merchant_cash/100,2),
            format_number(merchant_frozen/100,2)
        
        FROM
           opay_dw.app_opay_account_report_d
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
                        <h2>用户余额日报</h2>
                    </caption>
                </table>


                <table width="95%" class="table">
                    <thead>
                        <tr>
                           <th colspan="17" align="left">金额：奈拉</th>
                        </tr>
                        <tr>
                            <th rowspan="2" align="center">日期</th>
                            <th rowspan="2" align="center">total</th>
                            <th colspan="4" align="center">subtotal_account</th>
                            <th colspan="4" align="center">customer_account</th>
                            <th colspan="4" align="center">agent_account</th>
                            <th colspan="4" align="center">merchant_account</th>
                        </tr>
                        <tr>
                            <th align="center">bonus</th>
                            <th align="center">cash</th>
                            <th align="center">frozen</th>

                            <!--customer-->
                            <th align="center">subtotal</th>
                            <th align="center">bonus</th>
                            <th align="center">cash</th>
                            <th align="center">frozen</th>

                            <!--agent数据-->
                            <th align="center">subtotal</th>
                            <th align="center">bonus</th>
                            <th align="center">cash</th>
                            <th align="center">frozen</th>
                        
                            <!--merchant数据-->
                            <th align="center">subtotal</th>
                            <th align="center">bonus</th>
                            <th align="center">cash</th>
                            <th align="center">frozen</th>


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
                <td align="right">{13}</td>
                <td align="right">{14}</td>
                <td align="right">{15}</td>
                <td align="right">{16}</td>

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

    email_to = Variable.get("account_report_receivers").split()
    #email_to = ['bigdata@opay-inc.com']
    #email_to = ['shuzhen.liu@opay-inc.com']

    email_subject = '用户余额日报_{}'.format(ds)
    send_email(
        email_to
        , email_subject, html, mime_charset='utf-8')
    cursor.close()
    return


send_account_balance_report = PythonOperator(
    task_id='send_account_balance_report',
    python_callable=send_balance_report_email,
    provide_context=True,
    dag=dag
)
dwm_opay_account_df_prev_day_task >> app_opay_account_report_d_task
app_opay_account_report_d_task >> send_account_balance_report
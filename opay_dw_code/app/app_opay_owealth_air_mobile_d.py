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
    'app_opay_owealth_air_mobile_d',
    schedule_interval="00 18 * * *",
    default_args=args)

##----------------------------------------- 依赖 ---------------------------------------##
ods_sqoop_base_airtime_topup_record_hf_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_airtime_topup_record_hf_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour=18/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_hf/opay_transaction/airtime_topup_record",
        pt='{{macros.ds_add(ds, +1)}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_mobiledata_topup_record_hf_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_mobiledata_topup_record_hf_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour=18/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_hf/opay_transaction/mobiledata_topup_record",
        pt='{{macros.ds_add(ds, +1)}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"dag":dag, "db": "opay_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "dt={pt}".format(pt=ds), "timeout": "3000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw"
table_name = "app_opay_owealth_air_mobile_d"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def app_opay_owealth_air_mobile_d_sql_task(ds):
    HQL = '''
    
    set mapred.max.split.size=1000000;
        INSERT overwrite TABLE opay_dw.app_opay_owealth_air_mobile_d partition (dt='{pt}')
        SELECT 
               airtime_amt,
               airtime_c,
               mobiledata_amt,
               mobiledata_c
        FROM
          (SELECT dt,
                  count(1) airtime_c,
                  sum(amount) airtime_amt
           FROM opay_dw_ods.ods_sqoop_base_airtime_topup_record_hf
           WHERE dt='{pt}'
             AND hour='18'
             AND from_unixtime(unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss')+3600)>='{yesterday} 19:00:00'
             AND from_unixtime(unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss')+3600)<'{pt} 19:00:00'
             AND order_status='SUCCESS'
            group by dt) m
        LEFT JOIN
          (SELECT dt,
                  count(1) mobiledata_c,
                  sum(amount) mobiledata_amt
           FROM opay_dw_ods.ods_sqoop_base_mobiledata_topup_record_hf
           WHERE dt='{pt}'
             AND hour='18'
             AND from_unixtime(unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss')+3600)>='{yesterday} 19:00:00'
             AND from_unixtime(unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss')+3600)<'{pt} 19:00:00'
             AND order_status='SUCCESS'
           group by dt) m1 ON m.dt=m1.dt


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
    _sql = app_opay_owealth_air_mobile_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(airflow.macros.ds_add(ds, +1), db_name, table_name, hdfs_path, "false",
                                                 "true")


app_opay_owealth_air_mobile_d_task = PythonOperator(
    task_id='app_opay_owealth_air_mobile_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)


def send_air_mobile_report_email(ds, **kwargs):
    sql = '''
        SELECT
           dt,
           cast(airtime_amt/100 as decimal(20,2)) airtime_amt,
           airtime_c,
           cast(mobiledata_amt/100 as decimal(20,2)) mobiledata_amt,
           mobiledata_c

        FROM
           opay_dw.app_opay_owealth_air_mobile_d
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
                        <h2>话费流量充值(19：00-19：00)日报</h2>
                    </caption>
                </table>
                <table width="95%" class="table">
                    <thead>
                       <tr>
                       <th colspan="5" align="left">金额：奈拉</th>
                       </tr>
                        <tr>
                            <th align="center">日期</th>
                            <th align="center">话费成功交易额</th>
                            <th align="center">话费成功交易笔数</th>
                            <th align="center">流量成功交易额</th>
                            <th align="center">流量成功交易笔数</th>
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
                 <td align="right">{dt}</td>
                 <td align="right">{airtime_amt}</td>
                 <td align="right">{airtime_c}</td>
                 <td align="right">{mobiledata_amt}</td>
                 <td align="right">{mobiledata_c}</td>

        '''

        for data in data_list:
            [
                dt,
                airtime_amt,
                airtime_c,
                mobiledata_amt,
                mobiledata_c
            ] = list(data)

            row = row_fmt.format(
                dt=dt,
                airtime_amt=format(airtime_amt, ','),
                airtime_c=format(int(airtime_c), ','),
                mobiledata_amt=format(mobiledata_amt, ','),
                mobiledata_c=format(int(mobiledata_c), ',')
            )
            week = data[1]
            if week == '6' or week == '7':
                row_html += weekend_tr_fmt.format(row=row)
            else:
                row_html += tr_fmt.format(row=row)


    html = html_fmt.format(rows=row_html)
    # send mail

    email_to = Variable.get("air_mobile_receives").split()
    #email_to = ['shuzhen.liu@opay-inc.com']

    email_subject = '话费流量充值(19：00-19：00)_{}'.format(airflow.macros.ds_add(ds, +1))
    send_email(
        email_to
        , email_subject, html, mime_charset='utf-8')
    cursor.close()
    return


send_air_mobile_report = PythonOperator(
    task_id='send_air_mobile_report',
    python_callable=send_air_mobile_report_email,
    provide_context=True,
    dag=dag
)
ods_sqoop_base_airtime_topup_record_hf_prev_day_task >> app_opay_owealth_air_mobile_d_task
ods_sqoop_base_mobiledata_topup_record_hf_prev_day_task >> app_opay_owealth_air_mobile_d_task
app_opay_owealth_air_mobile_d_task >> send_air_mobile_report

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
    'start_date': datetime(2019, 12, 8),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_opay_owealth_collect_24_d',
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

ods_sqoop_owealth_owealth_user_subscribed_df_prev_day_task = OssSensor(
    task_id='ods_sqoop_owealth_owealth_user_subscribed_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_owealth_ods/opay_owealth/owealth_user_subscribed",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_owealth_share_revenue_log_df_prev_day_task = OssSensor(
    task_id='ods_sqoop_owealth_share_revenue_log_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_owealth_ods/opay_owealth/share_revenue_log",
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
table_name = "app_opay_owealth_collect_24_d"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def app_opay_owealth_collect_24_d_sql_task(ds):
    HQL = '''
        WITH acct_base AS
      (SELECT user_id,
              create_time,
              balance
       FROM opay_owealth_ods.ods_sqoop_owealth_share_acct_df
       WHERE dt='{pt}'
         AND from_unixtime(unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss')+3600)<'{pt} 24:00:00' ),
         order_base AS
      (SELECT create_time,
              order_type,
              trans_amount,
              user_id,
              memo
       FROM opay_owealth_ods.ods_sqoop_owealth_share_order_df
       WHERE dt='{pt}'
         AND status="S"
         AND from_unixtime(unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss')+3600)>'{pt}'
         AND from_unixtime(unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss')+3600)<'{pt} 24:00:00' ),
         user_subscribed AS
      (SELECT user_id,
              update_time,
              subscribed,
              mobile
       FROM opay_owealth_ods.ods_sqoop_owealth_owealth_user_subscribed_df
       WHERE dt='{pt}'
         AND from_unixtime(unix_timestamp(update_time, 'yyyy-MM-dd HH:mm:ss')+3600)<'{pt} 24:00:00' ),
         revenue AS
      (SELECT *
       FROM opay_owealth_ods.ods_sqoop_owealth_share_revenue_log_df
       WHERE dt='{pt}'
         AND from_unixtime(unix_timestamp(revenue_date, 'yyyy-MM-dd HH:mm:ss')+3600)>'{pt}'
         AND from_unixtime(unix_timestamp(revenue_date, 'yyyy-MM-dd HH:mm:ss')+3600)<'{pt} 24:00:00' ),
         user_role AS
      (SELECT user_id,
              ROLE,
              mobile
       FROM
         (SELECT user_id,
                 ROLE,
                 mobile,
                 row_number() over(partition BY user_id
                                   ORDER BY update_time DESC) rn
          FROM opay_dw_ods.ods_sqoop_base_user_di
          WHERE dt <= '{pt}' ) t1
       WHERE rn = 1)
    INSERT overwrite TABLE opay_dw.app_opay_owealth_collect_24_d partition (dt='{pt}')
    SELECT
    total_balance, --总的累计金额
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
     revenue_amount, --入账利息
     m.ROLE
    
    FROM
      (SELECT '{pt}' AS dt,
              ROLE,
              sum(balance) total_balance
       FROM acct_base a
       INNER JOIN user_role b ON a.user_id=b.mobile
       GROUP BY b.ROLE) m
    LEFT JOIN
      (SELECT '{pt}' AS dt,
              ROLE,
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
                                 WHEN memo='申购' THEN a.user_id
                             END) no_api_subscribe_user,
              count(DISTINCT CASE
                                 WHEN memo='API' THEN a.user_id
                             END) api_subscribe_user,
              sum(CASE
                      WHEN order_type='1002'
                           AND memo='赎回' THEN trans_amount
                  END) total_redeem_amount,
              count(DISTINCT CASE
                                 WHEN memo='赎回' THEN a.user_id
                             END) redeem_user
       FROM order_base a
       INNER JOIN user_role b ON a.user_id=b.mobile
       GROUP BY ROLE)m1 ON m.dt=m1.dt and m.ROLE=m1.role
    LEFT JOIN
      (SELECT '{pt}' AS dt,
              ROLE,
              count(DISTINCT CASE
                                 WHEN subscribed='Y' THEN a.user_id
                             END) add_open_api_subscribe_user
       FROM user_subscribed a
       INNER JOIN user_role b ON a.mobile=b.mobile
       GROUP BY ROLE) m2 ON m.dt=m2.dt and m.ROLE=m2.ROLE
    LEFT JOIN
      (SELECT '{pt}' AS dt,
              ROLE,
              count(DISTINCT CASE
                                 WHEN subscribed='Y' THEN a.user_id
                             END)open_api_subscribe_user,
              count(DISTINCT CASE
                                 WHEN subscribed='N' THEN a.user_id
                             END) close_api_subscribe_user
       FROM user_subscribed a
       INNER JOIN user_role b ON a.mobile=b.mobile
       WHERE from_unixtime(unix_timestamp(update_time, 'yyyy-MM-dd HH:mm:ss')+3600)>'{pt}'
       GROUP BY ROLE) m3 ON m.dt=m3.dt and m.ROLE=m3.ROLE
    LEFT JOIN
      (SELECT '{pt}' AS dt,
              ROLE,
              sum(revenue_amount) revenue_amount
       FROM revenue a
       INNER JOIN user_role b ON a.user_id=b.mobile
       GROUP BY ROLE) m4 ON m.dt=m4.dt and m.ROLE=m4.ROLE
   
   
    '''.format(
        pt=ds,
        db=db_name,
        table=table_name
    )

    return HQL


def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_opay_owealth_collect_24_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "false", "true")


app_opay_owealth_collect_24_d_task = PythonOperator(
    task_id='app_opay_owealth_collect_24_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

ods_sqoop_owealth_share_acct_df_prev_day_task >> app_opay_owealth_collect_24_d_task
ods_sqoop_owealth_share_order_df_prev_day_task >> app_opay_owealth_collect_24_d_task
ods_sqoop_owealth_owealth_user_subscribed_df_prev_day_task >> app_opay_owealth_collect_24_d_task
ods_sqoop_owealth_share_revenue_log_df_prev_day_task >> app_opay_owealth_collect_24_d_task
ods_sqoop_base_user_di_prev_day_task >> app_opay_owealth_collect_24_d_task







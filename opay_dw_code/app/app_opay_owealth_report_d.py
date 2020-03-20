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
    'start_date': datetime(2020, 3, 12),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_opay_owealth_report_d',
    schedule_interval="50 02 * * *",
    default_args=args)

##----------------------------------------- 依赖 ---------------------------------------##
# ods_sqoop_owealth_share_order_df_prev_day_task = OssSensor(
#     task_id='ods_sqoop_owealth_share_order_df_prev_day_task',
#     bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
#         hdfs_path_str="opay_owealth_ods/opay_owealth/share_order",
#         pt='{{ds}}'
#     ),
#     bucket_name='opay-datalake',
#     poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
#     dag=dag
# )

# ods_sqoop_owealth_owealth_user_subscribed_df_prev_day_task = OssSensor(
#     task_id='ods_sqoop_owealth_owealth_user_subscribed_df_prev_day_task',
#     bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
#         hdfs_path_str="opay_owealth_ods/opay_owealth/owealth_user_subscribed",
#         pt='{{ds}}'
#     ),
#     bucket_name='opay-datalake',
#     poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
#     dag=dag
# )

# ods_sqoop_owealth_owealth_share_trans_record_prev_day_task = OssSensor(
#     task_id='ods_sqoop_owealth_owealth_share_trans_record_prev_day_task',
#     bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
#         hdfs_path_str="opay_owealth_ods/opay_owealth/share_trans_record",
#         pt='{{ds}}'
#     ),
#     bucket_name='opay-datalake',
#     poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
#     dag=dag
# )

# ods_sqoop_base_user_di_prev_day_task = OssSensor(
#     task_id='ods_sqoop_base_user_di_prev_day_task',
#     bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
#         hdfs_path_str="opay_dw_sqoop_di/opay_user/user",
#         pt='{{ds}}'
#     ),
#     bucket_name='opay-datalake',
#     poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
#     dag=dag
# )


##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"dag": dag, "db": "opay_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "3000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw"
table_name = "app_opay_owealth_report_d"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def app_opay_owealth_report_d_sql_task(ds):
    HQL = '''

    set mapred.max.split.size=1000000;
    WITH 
    order_base AS
       (SELECT create_time,
              order_type,
              trans_amount,
              user_id,
              memo
        FROM opay_owealth_ods.ods_sqoop_owealth_share_order_df
        WHERE dt='2020-03-18'
         AND status="S"
         AND date_format(update_time,'yyyy-MM-dd')='{pt}'
        ),
    user_subscribed AS
       (SELECT user_id,
              date_format(update_time,'yyyy-MM-dd') update_date,
              subscribed,
              mobile
        FROM opay_owealth_ods.ods_sqoop_owealth_owealth_user_subscribed_df
        WHERE dt='2020-03-18'
          AND update_time<'{pt} 23:00:00' 
        ),
    share_trans_record AS
       (SELECT order_type,
               amount,
               user_id,
               date_format(create_time,'yyyy-MM-dd') create_date
        FROM opay_owealth_ods.ods_sqoop_owealth_share_trans_record_df
        WHERE dt='2020-03-18'
          AND create_time<'{pt} 24:00:00' 
        ),
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
       
    INSERT overwrite TABLE opay_dw.app_opay_owealth_report_d partition (country_code='NG',dt='{pt}')
    SELECT
         balance, --余额（总的申购-总的赎回+总的利息）
         curr_subscribe_amount,--当天申购金额
         curr_subscribe_cnt,--当天申购笔数
         curr_redeem_amount,--当天赎回金额
         curr_redeem_cnt,--当天赎回笔数
         no_api_subscribe_amount,--当天手动申购金额
         no_api_subscribe_cnt,--当天手动申购笔数
         api_subscribe_amount,--当天自动申购金额
         api_subscribe_cnt,--当天自动申购笔数
         no_api_subscribe_user,--当天手动申购交易用户数
         api_subscribe_user,--当天自动申购交易用户数
         redeem_user,--当天赎回交易用户数
         add_open_api_subscribe_user,--累计开通自动申购用户数
         open_api_subscribe_user,--当天开通自动申购用户数
         close_api_subscribe_user,--当天关闭自用申购用户数
         curr_revenue_amount, --当天入账利息
         m.ROLE

    FROM
      (SELECT '{pt}' AS dt,
              nvl(ROLE,'ALL') role,
              sum(case when order_type='1001' then amount end)-sum(case when order_type='1002' then amount end)
              +sum(case when order_type='1003' then amount end) balance,
              sum(case when create_date='{pt}' and order_type='1001' then amount end) curr_subscribe_amount,
              sum(case when create_date='{pt}' and order_type='1001' then 1 end) curr_subscribe_cnt,
              sum(case when create_date='{pt}' and order_type='1002' then amount end) curr_redeem_amount,
              sum(case when create_date='{pt}' and order_type='1002' then 1 end) curr_redeem_cnt,
              sum(case when create_date='{pt}' and order_type='1003' then amount end) curr_revenue_amount
       FROM share_trans_record a
       INNER JOIN user_role b ON a.user_id=b.mobile
       GROUP BY b.ROLE
       with cube) m
    LEFT JOIN
      (SELECT '{pt}' AS dt,
              nvl(ROLE,'ALL') role,
              sum(CASE WHEN order_type='1001' AND memo='申购' THEN trans_amount END) no_api_subscribe_amount,
              sum(CASE WHEN order_type='1001' AND memo='申购' THEN 1 END) no_api_subscribe_cnt,
              sum(CASE WHEN order_type='1001' AND memo='API' THEN trans_amount END) api_subscribe_amount,
              sum(CASE WHEN order_type='1001' AND memo='API' THEN 1 END) api_subscribe_cnt,
              count(DISTINCT CASE WHEN memo='申购' THEN a.user_id END) no_api_subscribe_user,
              count(DISTINCT CASE WHEN memo='API' THEN a.user_id END) api_subscribe_user,
              count(DISTINCT CASE WHEN memo='赎回' THEN a.user_id END) redeem_user
       FROM order_base a
       INNER JOIN user_role b ON a.user_id=b.mobile
       GROUP BY ROLE
       with cube)m1 ON m.dt=m1.dt and m.ROLE=m1.role
    LEFT JOIN
      (SELECT '{pt}' AS dt,
              nvl(role,'ALL') role,
              count(DISTINCT CASE WHEN subscribed='Y' THEN a.user_id END) add_open_api_subscribe_user,
              count(DISTINCT CASE WHEN update_date='{pt}' and subscribed='Y' THEN a.user_id END)open_api_subscribe_user,
              count(DISTINCT CASE WHEN update_date='{pt}' and subscribed='N' THEN a.user_id END) close_api_subscribe_user
       FROM user_subscribed a
       INNER JOIN user_role b ON a.mobile=b.mobile
       GROUP BY ROLE
       with cube) m2 ON m.dt=m2.dt and m.ROLE=m2.ROLE
    


    '''.format(
        pt=ds,
        db=db_name,
        table=table_name
    )

    return HQL


def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_opay_owealth_report_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


app_opay_owealth_report_d_task = PythonOperator(
    task_id='app_opay_owealth_report_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

# ods_sqoop_owealth_share_order_df_prev_day_task >> app_opay_owealth_report_d_task
# ods_sqoop_owealth_owealth_user_subscribed_df_prev_day_task >> app_opay_owealth_report_d_task
# ods_sqoop_owealth_owealth_share_trans_record_prev_day_task >> app_opay_owealth_report_d_task
# ods_sqoop_base_user_di_prev_day_task >> app_opay_owealth_report_d_task







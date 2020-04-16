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
from plugins.CountriesAppFrame import CountriesAppFrame

args = {
    'owner': 'xiedong',
    'start_date': datetime(2020, 3, 11),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_opay_owealth_report_d_19',
    schedule_interval="35 18 * * *",
    default_args=args)

##----------------------------------------- 依赖 ---------------------------------------##
ods_sqoop_owealth_share_order_hf_prev_day_task = OssSensor(
    task_id='ods_sqoop_owealth_share_order_hf_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour=18/_SUCCESS'.format(
        hdfs_path_str="opay_owealth_sqoop_hf/opay_owealth/share_order",
        pt='{{macros.ds_add(ds, +1)}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_owealth_user_subscribed_hf_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_owealth_user_subscribed_hf_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour=18/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_hf/opay_owealth/owealth_user_subscribed",
        pt='{{macros.ds_add(ds, +1)}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
ods_sqoop_base_owealth_share_trans_record_hf_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_owealth_share_trans_record_hf_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour=18/_SUCCESS'.format(
        hdfs_path_str="opay_owealth_sqoop_hf/opay_owealth/share_trans_record",
        pt='{{macros.ds_add(ds, +1)}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dim_opay_user_base_hf_check_task = OssSensor(
    task_id='dim_opay_user_base_hf_check_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour=18/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dim_opay_user_base_hf/country_code=NG",
        pt='{{macros.ds_add(ds, +1)}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"dag": dag, "db": "opay_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=NG/dt={pt}".format(pt=airflow.macros.ds_add(ds, +1)), "timeout": "3000"}
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
table_name = "app_opay_owealth_report_d_19"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def app_opay_owealth_report_d_19_sql_task(ds):
    HQL = '''

    set mapred.max.split.size=1000000;
    WITH 
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
          AND update_time>='{yesterday} 19:00:00'
          AND update_time<'{pt} 19:00:00' 
       ),
    user_subscribed AS
       (SELECT user_id,
          update_time,
          subscribed,
          mobile
        FROM opay_dw_ods.ods_sqoop_base_owealth_user_subscribed_hf
        WHERE dt='{pt}'
           AND hour='18'
           AND create_time<'{pt} 18:00:00' 
            ),
    share_trans_record AS
       (SELECT order_type,
               amount,
               user_id,
               create_time
        FROM opay_owealth_ods.ods_sqoop_owealth_share_trans_record_hf
        WHERE dt='{pt}'
           and hour='18'
          AND create_time<'{pt} 19:00:00' 
        ),
    user_role AS
       (SELECT user_id,
              ROLE,
              mobile
        FROM opay_dw.dim_opay_user_base_hf 
        where dt='{pt}'
           and hour='18'
           and create_time<'{pt} 19:00:00' 
         )
       
    INSERT overwrite TABLE opay_dw.app_opay_owealth_report_d_19 partition (country_code='NG',dt='{pt}')
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
              sum(case when create_time>'{yesterday} 19:00:00' and order_type='1001' then amount end) curr_subscribe_amount,
              sum(case when create_time>'{yesterday} 19:00:00' and order_type='1001' then 1 end) curr_subscribe_cnt,
              sum(case when create_time>'{yesterday} 19:00:00' and order_type='1002' then amount end) curr_redeem_amount,
              sum(case when create_time>'{yesterday} 19:00:00' and order_type='1002' then 1 end) curr_redeem_cnt,
              sum(case when create_time>'{yesterday} 19:00:00' and order_type='1003' then amount end) curr_revenue_amount
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
              count(DISTINCT CASE WHEN update_time>'{yesterday} 18:00:00' and subscribed='Y' THEN a.user_id END)open_api_subscribe_user,
              count(DISTINCT CASE WHEN update_time>'{yesterday} 18:00:00' and subscribed='N' THEN a.user_id END) close_api_subscribe_user
       FROM user_subscribed a
       INNER JOIN user_role b ON a.mobile=b.mobile
       GROUP BY ROLE
       with cube) m2 ON m.dt=m2.dt and m.ROLE=m2.ROLE
    



'''.format(
        pt=airflow.macros.ds_add(ds, +1),
        yesterday=ds,
        db=db_name,
        table=table_name
    )

    return HQL


# 主流程
def execution_data_task_id(ds, dag, **kwargs):
    v_execution_time = kwargs.get('v_execution_time')
    hive_hook = HiveCliHook()

    args = [
        {
            "dag": dag,
            "is_countries_online": "true",
            "db_name": db_name,
            "table_name": table_name,
            "data_oss_path": hdfs_path,
            "is_country_partition": "true",
            "is_result_force_exist": "false",
            "execute_time": v_execution_time,
            "is_hour_task": "false",
            "frame_type": "local",
            "business_key": "opay"
        }
    ]

    cf = CountriesAppFrame(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + app_opay_owealth_report_d_19_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


app_opay_owealth_report_d_19_task = PythonOperator(
    task_id='app_opay_owealth_report_d_19_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_time': '{{(execution_date + macros.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

ods_sqoop_base_owealth_share_trans_record_hf_prev_day_task >> app_opay_owealth_report_d_19_task
ods_sqoop_owealth_share_order_hf_prev_day_task >> app_opay_owealth_report_d_19_task
ods_sqoop_base_owealth_user_subscribed_hf_prev_day_task >> app_opay_owealth_report_d_19_task
dim_opay_user_base_hf_check_task >> app_opay_owealth_report_d_19_task
#app_opay_owealth_report_d_19_task >> send_owealth_report



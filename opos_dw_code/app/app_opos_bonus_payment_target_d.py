# -*- coding: utf-8 -*-
import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
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
    'owner': 'yuanfeng',
    'start_date': datetime(2019, 11, 24),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opos_bonus_payment_target_d',
                  schedule_interval="10 03 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区，ods_sqoop_base_pre_opos_payment_order_di表，ufile://opay-datalake/opos_dw_sqoop_di/pre_ptsp_db/pre_opos_payment_order
dwd_pre_opos_payment_order_di_task = UFileSensor(
    task_id='dwd_pre_opos_payment_order_di_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos/opos_dw/dwd_pre_opos_payment_order_di",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "opos_dw"
table_name = "app_opos_bonus_payment_target_d"
hdfs_path = "ufile://opay-datalake/opos/opos_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "opos_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def app_opos_bonus_payment_target_d_sql_task(ds):
    HQL = '''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    --02.先取红包record表
    insert overwrite table opos_dw.app_opos_bonus_payment_target_d partition(country_code,dt)
    select
    create_date,
    create_week,
    create_month,
    create_year,

    city_id as city_code,
    city_name,
    country,

    cm_id,
    cm_name,
    rm_id,
    rm_name,
    bdm_id,
    bdm_name,
    bd_id,
    bd_name,

    --红包订单数量
    count(if(length(discount_ids)>0,1,null)) as bonus_order_cnt,
    --红包订单GMV
    sum(if(length(discount_ids)>0,nvl(org_payment_amount,0),0)) as bonus_order_gmv,
    --首单用户占比（红包）,判断是否是首单需要确认,暂定0和1
    count(distinct(if(length(discount_ids)>0 and first_order='1',sender_id,null)))/count(distinct(if(length(discount_ids)>0,sender_id,null))) as first_order_percent,
    --红包支付用户数
    count(distinct(if(length(discount_ids)>0,sender_id,null))) as bonus_order_people,
    --红包支付次数
    count(if(length(discount_ids)>0,1,null)) as bonus_order_times,

    --人均消费金额增幅（红包用户）,用红包支付的人均应付金额-没用红包支付的人均应付金额
    sum(if(length(discount_ids)>0,nvl(org_payment_amount,0),0))/count(distinct(if(length(discount_ids)>0,sender_id,null))) 
      - sum(if(length(discount_ids)=0,nvl(org_payment_amount,0),0))/count(distinct(if(length(discount_ids)=0,sender_id,null))) as people_avg_amt_increase,
    --订单均消费金额增幅（红包用户）,用红包支付的单均应付金额-没用红包支付的单均应付金额,这个分母不需要加distinct,
    sum(if(length(discount_ids)>0,nvl(org_payment_amount,0),0))/count(if(length(discount_ids)>0,sender_id,null)) 
      - sum(if(length(discount_ids)=0,nvl(org_payment_amount,0),0))/count(if(length(discount_ids)=0,sender_id,null)) as order_avg_amt_increase,
    --单均实付增幅（红包用户）,用实付
    sum(if(length(discount_ids)>0,nvl(pay_amount,0),0))/count(if(length(discount_ids)>0,sender_id,null)) 
      - sum(if(length(discount_ids)=0,nvl(pay_amount,0),0))/count(if(length(discount_ids)=0,sender_id,null)) as order_avg_realamt_increase,
    --交易频次增幅（红包用户）,【使用红包支付订单的】交易频次-【不使用红包支付订单的】交易频次
    count(if(length(discount_ids)>0,1,null)) 
      - count(if(length(discount_ids)=0,1,null)) as order_times_increase,
    --首单用户数（红包用户）
    count(distinct(if(length(discount_ids)>0 and first_order='1',sender_id,null))) as first_order_people,

    --新增字段
    --没用红包支付的应付金额
    sum(if(length(discount_ids)=0,nvl(org_payment_amount,0),0)) as useless_org_payment_amt,
    --没用红包支付的用户数
    count(distinct(if(length(discount_ids)=0,sender_id,null))) as useless_people,
    --没用红包支付的订单数
    count(if(length(discount_ids)=0,sender_id,null)) as useless_order_cnt,

    --用红包支付的实付总金额
    sum(if(length(discount_ids)>0,nvl(pay_amount,0),0)) as bonus_real_amt,
    --没用红包支付的实付总金额
    sum(if(length(discount_ids)=0,nvl(pay_amount,0),0)) as useless_bonus_real_amt,

    'nal' as country_code,
    '{pt}' as dt

    from
    opos_dw.dwd_pre_opos_payment_order_di
    where 
    country_code='nal' 
    and dt='{pt}'
    and trade_status='SUCCESS'
    group by
    create_date,
    create_week,
    create_month,
    create_year,

    city_id,
    city_name,
    country,

    cm_id,
    cm_name,
    rm_id,
    rm_name,
    bdm_id,
    bdm_name,
    bd_id,
    bd_name;



'''.format(
        pt=ds,
        table=table_name,
        now_day='{{macros.ds_add(ds, +1)}}',
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_opos_bonus_payment_target_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    # check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


app_opos_bonus_payment_target_d_task = PythonOperator(
    task_id='app_opos_bonus_payment_target_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_pre_opos_payment_order_di_task >> app_opos_bonus_payment_target_d_task

# 查看任务命令
# airflow list_tasks app_opos_bonus_payment_target_d -sd /home/feng.yuan/app_opos_bonus_payment_target_d.py
# 测试任务命令
# airflow test app_opos_bonus_payment_target_d app_opos_bonus_payment_target_d_task 2019-11-24 -sd /home/feng.yuan/app_opos_bonus_payment_target_d.py




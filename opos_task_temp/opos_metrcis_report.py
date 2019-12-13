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
from airflow.sensors import OssSensor
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'yuanfeng',
    'start_date': datetime(2019, 11, 28),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('opos_metrcis_report',
                  schedule_interval="50 03 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

dwd_pre_opos_payment_order_di_task = OssSensor(
    task_id='dwd_pre_opos_payment_order_di_task',
    bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos/opos_dw/dwd_pre_opos_payment_order_di",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "opos_temp"
table_name = "opos_metrcis_report"
hdfs_path = "oss://opay-datalake/opos/opos_temp/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "opos_temp", "table": "{dag_name}".format(dag_name=dag_ids),
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

def opos_metrcis_report_sql_task(ds):
    HQL = '''

set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;

--01.清洗订单明细表,用order_id,即收款方id关联opay_id,将每笔交易关联上bd_id后
--根据bdid与城市编码分组,统计各个指标值
--每天将历史数据和最新一天的数据累加，计算后放入历史表中,因为会有新增的情况，故需要用fulljoin
insert overwrite table opos_temp.app_opos_order_data_history_di partition(country_code,dt)
select 
hcm_id
,cm_id
,rm_id
,bdm_id
,bd_id

,city_id

,nvl(count(if(order_type = 'pos',order_id,null)),0) as his_pos_complete_order_cnt
,nvl(count(if(order_type = 'qrcode',order_id,null)),0) as his_qr_complete_order_cnt
,nvl(count(order_id),0) as his_complete_order_cnt
,nvl(sum(nvl(org_payment_amount,0)),0) as his_gmv
,nvl(sum(nvl(pay_amount,0)),0) as his_actual_amount
,nvl(sum(nvl(return_amount,0)),0) as his_return_amount
,nvl(sum(if(first_order = '1',nvl(org_payment_amount,0) - nvl(pay_amount,0) + nvl(user_subsidy,0),0)),0) as his_new_user_cost
,nvl(sum(if(first_order <> '1',nvl(org_payment_amount,0) - nvl(pay_amount,0) + nvl(user_subsidy,0),0)),0) as his_old_user_cost
,nvl(count(if(return_amount > 0,order_id,null)),0) as his_return_amount_order_cnt

,nvl(count(if(dt = '{pt}' and order_type = 'pos',order_id,null)),0) as pos_complete_order_cnt
,nvl(count(if(dt = '{pt}' and order_type = 'qrcode',order_id,null)),0) as qr_complete_order_cnt
,nvl(count(if(dt = '{pt}',order_id,null)),0) as complete_order_cnt
,nvl(sum(if(dt = '{pt}',nvl(org_payment_amount,0),0)),0) as gmv
,nvl(sum(if(dt = '{pt}',nvl(pay_amount,0),0)),0) as actual_amount
,nvl(sum(if(dt = '{pt}',nvl(return_amount,0),0)),0) as return_amount
,nvl(sum(if(dt = '{pt}' and first_order = '1',nvl(org_payment_amount,0) - nvl(pay_amount,0) + nvl(user_subsidy,0),0)),0) as new_user_cost
,nvl(sum(if(dt = '{pt}' and first_order <> '1',nvl(org_payment_amount,0) - nvl(pay_amount,0) + nvl(user_subsidy,0),0)),0) as old_user_cost
,nvl(count(if(dt = '{pt}' and return_amount > 0,order_id,null)),0) as return_amount_order_cnt

,'nal' as country_code
,'{pt}' as dt

from 
opos_dw.dwd_pre_opos_payment_order_di as p
where 
country_code='nal' 
and dt >= '2019-10-25' 
and dt <= '{pt}' 
and trade_status = 'SUCCESS'
group by
hcm_id
,cm_id
,rm_id
,bdm_id
,bd_id

,city_id
;

--02.用shop表计算出每个bd下有
--得出最新维度下每个dbid的详细数据信息
insert overwrite table opos_temp.opos_metrcis_report partition (country_code,dt)
select
nvl(a.hcm_id,b.hcm_id) as hcm_id
,nvl(a.cm_id,b.cm_id) as cm_id
,nvl(a.rm_id,b.rm_id) as rm_id
,nvl(a.bdm_id,b.bdm_id) as bdm_id
,nvl(a.bd_id,b.bd_id) as bd_id

,nvl(a.city_id,b.city_id) as city_id

,nvl(a.merchant_cnt,0) as merchant_cnt
,nvl(a.pos_merchant_cnt,0) as pos_merchant_cnt
,nvl(a.new_merchant_cnt,0) as new_merchant_cnt
,nvl(a.new_pos_merchant_cnt,0) as new_pos_merchant_cnt

,nvl(b.his_pos_complete_order_cnt,0) as his_pos_complete_order_cnt
,nvl(b.his_qr_complete_order_cnt,0) as his_qr_complete_order_cnt
,nvl(b.his_complete_order_cnt,0) as his_complete_order_cnt
,nvl(b.his_gmv,0) as his_gmv
,nvl(b.his_actual_amount,0) as his_actual_amount
,nvl(b.his_return_amount,0) as his_return_amount
,nvl(b.his_new_user_cost,0) as his_new_user_cost
,nvl(b.his_old_user_cost,0) as his_old_user_cost
,nvl(b.his_return_amount_order_cnt,0) as his_return_amount_order_cnt

,nvl(b.pos_complete_order_cnt,0) as pos_complete_order_cnt
,nvl(b.qr_complete_order_cnt,0) as qr_complete_order_cnt
,nvl(b.complete_order_cnt,0) as complete_order_cnt
,nvl(b.gmv,0) as gmv
,nvl(b.actual_amount,0) as actual_amount
,nvl(b.return_amount,0) as return_amount
,nvl(b.new_user_cost,0) as new_user_cost
,nvl(b.old_user_cost,0) as old_user_cost
,nvl(b.return_amount_order_cnt,0) as return_amount_order_cnt

,'nal' as country_code
,'{pt}' as dt
from
  (select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id
  
  ,city_code as city_id
  
  ,count(id) as merchant_cnt
  ,0 as pos_merchant_cnt
  ,count(if(created_at = '{pt}',id,null)) as new_merchant_cnt
  ,0 as new_pos_merchant_cnt
  from
  opos_dw.dim_opos_bd_relation_df
  where 
  country_code='nal' and dt='{pt}'
  group by
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id
  
  ,city_code
  ) as a
full join
  (select * from opos_temp.app_opos_order_data_history_di where country_code='nal' and dt='{pt}') as b
on
a.hcm_id=b.hcm_id
AND a.cm_id=b.cm_id
AND a.rm_id=b.rm_id
AND a.bdm_id=b.bdm_id
AND a.bd_id=b.bd_id
and a.city_id=b.city_id
;






'''.format(
        pt=ds,
        table=table_name,
        now_day=airflow.macros.ds_add(ds, +1),
        before_1_day=airflow.macros.ds_add(ds, -1),
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = opos_metrcis_report_sql_task(ds)

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


opos_metrcis_report_task = PythonOperator(
    task_id='opos_metrcis_report_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_pre_opos_payment_order_di_task >> opos_metrcis_report_task

# 查看任务命令
# airflow list_tasks opos_metrcis_report -sd /home/feng.yuan/opos_metrcis_report.py
# 测试任务命令
# airflow test opos_metrcis_report opos_metrcis_report_task 2019-11-24 -sd /home/feng.yuan/opos_metrcis_report.py



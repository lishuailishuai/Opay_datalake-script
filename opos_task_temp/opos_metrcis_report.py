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
    'start_date': datetime(2019, 11, 25),
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

# 依赖前一天分区，dim_opos_bd_relation_df表，ufile://opay-datalake/opos/opos_dw/dim_opos_bd_relation_df
dim_opos_bd_relation_df_task = UFileSensor(
    task_id='dim_opos_bd_relation_df_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos/opos_dw/dim_opos_bd_relation_df",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_pre_opos_payment_order_di_task = UFileSensor(
    task_id='ods_sqoop_base_pre_opos_payment_order_di_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos_dw_sqoop_di/pre_ptsp_db/pre_opos_payment_order",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_pre_opos_payment_order_bd_di_task = UFileSensor(
    task_id='ods_sqoop_base_pre_opos_payment_order_bd_di_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos_dw_sqoop_di/pre_ptsp_db/pre_opos_payment_order_bd",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "opos_temp"
table_name = "opos_metrcis_report"
hdfs_path = "ufile://opay-datalake/opos/opos_temp/" + table_name


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
nvl(a.cm_id,b.cm_id) as cm_id
,nvl(a.cm_name,b.cm_name) as cm_name
,nvl(a.rm_id,b.rm_id) as rm_id
,nvl(a.rm_name,b.rm_name) as rm_name
,nvl(a.bdm_id,b.bdm_id) as bdm_id
,nvl(a.bdm_name,b.bdm_name) as bdm_name
,nvl(a.bd_id,b.bd_id) as bd_id
,nvl(a.bd_name,b.bd_name) as bd_name
,nvl(a.city_id,b.city_id) as city_id
,nvl(a.city_name,b.city_name) as city_name
,nvl(a.country,b.country) as country

,nvl(a.his_pos_complete_order_cnt,0) + nvl(b.his_pos_complete_order_cnt,0)  as his_pos_complete_order_cnt
,nvl(a.his_qr_complete_order_cnt,0) + nvl(b.his_qr_complete_order_cnt,0)  as his_qr_complete_order_cnt
,nvl(a.his_complete_order_cnt,0) + nvl(b.his_complete_order_cnt,0)  as his_complete_order_cnt
,nvl(a.his_gmv,0) + nvl(b.his_gmv,0)  as his_gmv
,nvl(a.his_actual_amount,0) + nvl(b.his_actual_amount,0)  as his_actual_amount
,nvl(a.his_return_amount,0) + nvl(b.his_return_amount,0)  as his_return_amount
,nvl(a.his_new_user_cost,0) + nvl(b.his_new_user_cost,0)  as his_new_user_cost
,nvl(a.his_old_user_cost,0) + nvl(b.his_old_user_cost,0)  as his_old_user_cost
,nvl(a.his_return_amount_order_cnt,0) + nvl(b.his_return_amount_order_cnt,0)  as his_return_amount_order_cnt

,nvl(b.pos_complete_order_cnt,0)  as pos_complete_order_cnt
,nvl(b.qr_complete_order_cnt,0)  as qr_complete_order_cnt
,nvl(b.complete_order_cnt,0)  as complete_order_cnt
,nvl(b.gmv,0)  as gmv
,nvl(b.actual_amount,0)  as actual_amount
,nvl(b.return_amount,0)  as return_amount
,nvl(b.new_user_cost,0)  as new_user_cost
,nvl(b.old_user_cost,0)  as old_user_cost
,nvl(b.return_amount_order_cnt,0)  as return_amount_order_cnt

,'nal' as country_code
,'{pt}' as dt
from
(select * from opos_temp.app_opos_order_data_history_di where country_code='nal' and dt='{before_1_day}' and length(bd_id)>0) as a
full join
(
select 
cm_id,
cm_name,
rm_id,
rm_name,
bdm_id,
bdm_name,
bd_id,
bd_name,

city_id, 
name as city_name,
country,

count(if(order_type = 'pos',order_id,null)) as his_pos_complete_order_cnt,
count(if(order_type = 'qrcode',order_id,null)) as his_qr_complete_order_cnt,
count(order_id) as his_complete_order_cnt,
sum(org_payment_amount) as his_gmv,
sum(pay_amount) as his_actual_amount,
sum(return_amount) as his_return_amount,
sum(if(first_order = '1',org_payment_amount - pay_amount + user_subsidy,0)) as his_new_user_cost,
sum(if(first_order <> '1',org_payment_amount - pay_amount + user_subsidy,0)) as his_old_user_cost,
count(if(return_amount > 0,order_id,null)) as his_return_amount_order_cnt,

count(if(dt = '{pt}' and order_type = 'pos',order_id,null)) as pos_complete_order_cnt,
count(if(dt = '{pt}' and order_type = 'qrcode',order_id,null)) as qr_complete_order_cnt,
count(if(dt = '{pt}',order_id,null)) as complete_order_cnt,
sum(if(dt = '{pt}',org_payment_amount,0)) as gmv,
sum(if(dt = '{pt}',pay_amount,0)) as actual_amount,
sum(if(dt = '{pt}',return_amount,0)) as return_amount,
sum(if(dt = '{pt}' and first_order = '1',org_payment_amount - pay_amount + user_subsidy,0)) as new_user_cost,
sum(if(dt = '{pt}' and first_order <> '1',org_payment_amount - pay_amount + user_subsidy,0)) as old_user_cost,
count(if(dt = '{pt}' and return_amount > 0,order_id,null)) as return_amount_order_cnt

from 
(select
p.dt,p.receipt_id,p.order_id,p.order_type,p.trade_status,p.org_payment_amount,p.pay_amount,p.return_amount,p.first_order,p.user_subsidy,bd.bd_id,bd.city_id,bd.name,bd.country,bd.cm_id,bd.cm_name,bd.rm_id,bd.rm_name,bd.bdm_id,bd.bdm_name,bd.bd_name
from
    (select dt,receipt_id,order_id,order_type,trade_status,org_payment_amount,pay_amount,return_amount,first_order,user_subsidy from opos_dw_ods.ods_sqoop_base_pre_opos_payment_order_di where dt = '{pt}' and trade_status = 'SUCCESS') as p 
inner join
    --先用orderod关联每一笔交易的bd_id,只取能关联上bd信息的交易，故用inner join
    (
    select s.order_id,s.bd_id,s.city_id,ci.name,ci.country,b.cm_id,b.cm_name,b.rm_id,b.rm_name,b.bdm_id,b.bdm_name,b.bd_name from
      (select order_id,bd_id,city_id from opos_dw_ods.ods_sqoop_base_pre_opos_payment_order_bd_di where dt='{pt}') as s 
    left join
    --关联城市码表，求出国家和城市描述
      (select id,name,country from opos_dw_ods.ods_sqoop_base_bd_city_df where dt = '{pt}') as ci
    on s.city_id=ci.id
    left join
    --关联bd信息码表，求出所有bd的层级关系和描述
      (select cm_id,cm_name,rm_id,rm_name,bdm_id,bdm_name,bd_id,bd_name from opos_dw.dim_opos_bd_info_df where country_code='nal' and   dt='{pt}')   as b
    on s.bd_id=b.bd_id
    ) as bd
on
    p.order_id=bd.order_id) as tmp

group by 
cm_id,cm_name,rm_id,rm_name,bdm_id,bdm_name,bd_id,bd_name,city_id, name,country
) as b
on
a.bd_id=b.bd_id
and a.city_id=b.city_id
;

--02.用shop表计算出每个bd下有
--得出最新维度下每个dbid的详细数据信息
insert overwrite table opos_temp.opos_metrcis_report partition (country_code,dt)
select
bd.cm_id
,bd.cm_name
,bd.rm_id
,bd.rm_name
,bd.bdm_id
,bd.bdm_name
,bd.bd_id
,bd.bd_name

,bd.city_id
,bd.city_name
,bd.country

,nvl(bd.merchant_cnt,0) merchant_cnt
,nvl(bd.pos_merchant_cnt,0) pos_merchant_cnt
,nvl(bd.new_merchant_cnt,0) new_merchant_cnt
,nvl(bd.new_pos_merchant_cnt,0) new_pos_merchant_cnt

,nvl(ord.pos_complete_order_cnt,0) pos_complete_order_cnt
,nvl(ord.qr_complete_order_cnt,0) qr_complete_order_cnt
,nvl(ord.complete_order_cnt,0) complete_order_cnt
,nvl(ord.gmv,0) gmv
,nvl(ord.actual_amount,0) actual_amount
,nvl(ord.return_amount,0) return_amount
,nvl(ord.new_user_cost,0) new_user_cost
,nvl(ord.old_user_cost,0) old_user_cost
,nvl(ord.return_amount_order_cnt,0) return_amount_order_cnt

,nvl(ord.his_pos_complete_order_cnt,0) his_pos_complete_order_cnt
,nvl(ord.his_qr_complete_order_cnt,0) his_qr_complete_order_cnt
,nvl(ord.his_complete_order_cnt,0) his_complete_order_cnt
,nvl(ord.his_gmv,0) his_gmv
,nvl(ord.his_actual_amount,0) his_actual_amount
,nvl(ord.his_return_amount,0) his_return_amount
,nvl(ord.his_new_user_cost,0) his_new_user_cost
,nvl(ord.his_old_user_cost,0) his_old_user_cost
,nvl(ord.his_return_amount_order_cnt,0) his_return_amount_order_cnt

,'nal' as country_code
,'{pt}' as dt
from
(select 
cm_id
,cm_name
,rm_id
,rm_name
,bdm_id
,bdm_name
,bd_id
,bd_name

,city_code as city_id
,city_name
,country

,count(id) as merchant_cnt
,0 as pos_merchant_cnt
,count(if(created_at = '{pt}',id,null)) as new_merchant_cnt
,0 as new_pos_merchant_cnt
from
opos_dw.dim_opos_bd_relation_df
where 
country_code='nal' 
and dt='{pt}'
and length(bd_id)>0
group by
cm_id
,cm_name
,rm_id
,rm_name
,bdm_id
,bdm_name
,bd_id
,bd_name

,city_code
,city_name
,country
) as bd
left join
(select * from opos_temp.app_opos_order_data_history_di where country_code='nal' and dt='{pt}') as ord
on
bd.bd_id = ord.bd_id
and bd.city_id = ord.city_id
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

dim_opos_bd_relation_df_task >> opos_metrcis_report_task
ods_sqoop_base_pre_opos_payment_order_di_task >> opos_metrcis_report_task
ods_sqoop_base_pre_opos_payment_order_bd_di_task >> opos_metrcis_report_task

# 查看任务命令
# airflow list_tasks opos_metrcis_report -sd /home/feng.yuan/opos_metrcis_report.py
# 测试任务命令
# airflow test opos_metrcis_report opos_metrcis_report_task 2019-11-24 -sd /home/feng.yuan/opos_metrcis_report.py



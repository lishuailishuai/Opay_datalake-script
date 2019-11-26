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

dag = airflow.DAG(
    'opos_order_metrics_daily',
    schedule_interval="10 04 * * *",
    default_args=args)

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
table_name = "opos_active_user_daily"
hdfs_path = "ufile://opay-datalake/oride/opos_temp/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "opos_temp", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


##----------------------------------------- 脚本 ---------------------------------------##

def opos_active_user_daily_sql_task(ds):
    HQL = '''
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;

--03.创建临时明细表,扩充明细信息,将最新一天的交易明细数据关联上dbid和地市编码名称后插入到临时表中
insert overwrite table opos_temp.opos_active_user_detail_daily partition (country_code,dt)
select 
b.cm_id
,b.cm_name
,b.rm_id
,b.rm_name
,b.bdm_id
,b.bdm_name
,s.bd_id
,b.bd_name

,s.city_id
,ci.name as city_name
,ci.country

,p.sender_id
,p.receipt_id
,p.order_type
,p.trade_status
,p.first_order

,'nal' as country_code
,p.dt
from 
(select dt,sender_id,receipt_id,order_id,order_type,trade_status,first_order from opos_dw_ods.ods_sqoop_base_pre_opos_payment_order_di where dt = '{pt}' and trade_status = 'SUCCESS'
) p 
left join
--先用orderod关联每一笔交易的bdid
(
select order_id,bd_id,city_id from opos_dw_ods.ods_sqoop_base_pre_opos_payment_order_bd_di where dt='{pt}'
) as s 
on 
p.order_id = s.order_id
left join
--关联城市码表，求出国家和城市描述
(
select id,name,country from opos_dw_ods.ods_sqoop_base_bd_city_df where dt = '{pt}'
) as ci
on
s.city_id=ci.id
left join
--关联bd信息码表，求出所有bd的层级关系和描述
(
select * from opos_dw.dim_opos_bd_info_df where country_code='nal' and dt='{pt}'
) as b
on
s.bd_id=b.bd_id
where
s.bd_id is not null;

--03.01.查出临时表中昨天,前天,7天前,15天前,30天前的数据
with
active_base as (
  select 
  * 
  from 
  opos_temp.opos_active_user_detail_daily
  where 
  country_code = 'nal' 
  and dt in ('{pt}','{before_1_day}','{before_7_day}','{before_15_day}','{before_30_day}')
),

--03.02.然后从昨天,前天等等特殊历史天数中每个付款客户及对应付款店dbid的次数
user_base as ( 
  select  
  cm_id
  ,cm_name
  ,rm_id
  ,rm_name
  ,bdm_id
  ,bdm_name
  ,bd_id
  ,bd_name 

  ,city_id
  ,city_name
  ,country

  ,sender_id
  ,order_type
  ,sum(if(dt = '{pt}',1,0)) as is_current_day
  ,sum(if(dt ='{before_1_day}',1,0)) as is_before_1_day
  ,sum(if(dt = '{before_7_day}',1,0)) as is_before_7_day
  ,sum(if(dt = '{before_15_day}',1,0)) as is_before_15_day
  ,sum(if(dt = '{before_30_day}',1,0)) as is_before_30_day
  ,sum(if(dt ='{pt}' and first_order = '1',1,0)) as is_new
  from 
  active_base
  group by 
  cm_id
  ,cm_name
  ,rm_id
  ,rm_name
  ,bdm_id
  ,bdm_name
  ,bd_id
  ,bd_name 

  ,city_id
  ,city_name
  ,country

  ,sender_id
  ,order_type
),

--03.03.统计昨日有交易的交易,然后统计每个dbid下的每种订单类型有多数个用户
current_user as (
  select 
  '{pt}' as dt
  ,cm_id
  ,cm_name
  ,rm_id
  ,rm_name
  ,bdm_id
  ,bdm_name
  ,bd_id
  ,bd_name 

  ,city_id
  ,city_name
  ,country

  ,count(distinct sender_id) as user_active_cnt
  ,count(distinct if(order_type = 'pos',sender_id,null)) as pos_user_active_cnt
  ,count(distinct if(order_type = 'qrcode',sender_id,null)) as qr_user_active_cnt
  ,count(distinct if(is_new > 0,sender_id,null)) as new_user_cnt

  from 
  user_base 
  where 
  is_current_day > 0
  group by 
  cm_id
  ,cm_name
  ,rm_id
  ,rm_name
  ,bdm_id
  ,bdm_name
  ,bd_id
  ,bd_name 

  ,city_id
  ,city_name
  ,country
),

--03.04.统计近两天都有交易的客户对应到dbid下的人数
day_1_remain as (

  select 
  '{pt}' as dt,
  bd_id,
  city_id,
  count(distinct sender_id) as user_active_cnt
  from 
  user_base 
  where is_current_day > 0 and is_before_1_day > 0
  group by 
  bd_id,
  city_id
),

--03.05.统计昨天和7天前都有交易的客户对应到dbid下的人数
day_7_remain as (

  select 
  '{pt}' as dt,
  bd_id,
  city_id,
  count(distinct sender_id) as user_active_cnt
  from 
  user_base 
  where is_current_day > 0 and is_before_7_day > 0
  group by 
  bd_id,
  city_id
),

--03.06.统计昨天和15天前都有交易的客户
day_15_remain as (
  select 
  '{pt}' as dt,
  bd_id,
  city_id,
  count(distinct sender_id) as user_active_cnt
  from 
  user_base 
  where is_current_day > 0 and is_before_15_day > 0
  group by 
  bd_id,
  city_id
),

--03.07.统计昨天和30天前都有交易的客户
day_30_remain as (
  select 
  '{pt}' as dt,
  bd_id,
  city_id,
  count(distinct sender_id) as user_active_cnt
  from 
  user_base 
  where is_current_day > 0 and is_before_30_day > 0
  group by 
  bd_id,
  city_id
),

--03.08.统计每个dbid下交易成功的商户数量以及用pos交易的商户数量
order_merchant_data as (
  select 
  '{pt}' as dt,
  bd_id,
  city_id,
  count(distinct(receipt_id)) as order_merchant_cnt,
  count(distinct(if(order_type = 'pos',receipt_id,null))) as pos_order_merchant_cnt
  from 
  active_base
  where dt = '{pt}'
  group by 
  bd_id,
  city_id
),

--03.09.join是inner join取维内自然周的数据
week_data as (
  select 
  '{pt}' as dt,
  u.bd_id,
  u.city_id,
  mt.monday_of_year,
  count(distinct(if(order_type = 'pos',sender_id,null))) as pos_user_active_cnt,
  count(distinct(if(order_type = 'qrcode',sender_id,null))) as qr_user_active_cnt
  from 
  (
  select 
  *
  from 
  opos_temp.opos_active_user_detail_daily 
  where 
  country_code = 'nal' 
  and dt >= '{pt}' 
  and dt <= '{before_15_day}'
  ) u 
  inner join 
  --取出当日所在日期的本周的所有7天的日期
  (select d.dt,d.monday_of_year from public_dw_dim.dim_date d inner join (select * from public_dw_dim.dim_date where dt = '{pt}') t on d.monday_of_year = t.monday_of_year) as mt

  on 
  u.dt = mt.dt
  group by 
  mt.monday_of_year
  ,u.bd_id
  ,u.city_id
),

--03.10.求出每月的dbid分别是pos和qrcode交易的笔数,只拿出所属月的
month_data as (
  select 
  '{pt}' as dt,
  u.bd_id,
  u.city_id,
  mt.month,
  count(distinct(if(order_type = 'pos',sender_id,null))) as pos_user_active_cnt,
  count(distinct(if(order_type = 'qrcode',sender_id,null))) as qr_user_active_cnt

  from 
  (
  select 
  *
  from 
  opos_temp.opos_active_user_detail_daily 
  where country_code = 'nal' 
  and dt <= '{pt}' 
  and dt >= '{before_30_day}'
  ) u 
  inner join 
  --取出当日所在日期的本月的所有的日期
  (select d.dt,d.month from public_dw_dim.dim_date d inner join (select * from public_dw_dim.dim_date where dt = '{pt}') t on d.month = t.month) as mt
  on 
  u.dt = mt.dt
  group by 
  mt.month,
  u.bd_id,
  u.city_id
),

--03.11.取出当天所有付款者的数量
have_order_user as (
  select 
  '{pt}' as dt,
  bd_id,
  city_id,
  count(distinct(sender_id)) as have_order_user_cnt
  from 
  active_base
  where 
  dt = '{pt}'
  group by 
  bd_id,
  city_id
),

--03.12.取出历史数据中所有的付款者，收款者，以及pos和扫码的人数
his_data as (
  select 
  '{pt}' as dt,
  bd_id,
  city_id,
  count(distinct(receipt_id)) as his_order_merchant_cnt,
  count(distinct(if(order_type = 'pos',receipt_id,null))) as his_pos_order_merchant_cnt,

  count(distinct(sender_id)) as his_user_active_cnt,
  count(distinct(if(order_type = 'pos',sender_id,null))) as his_pos_user_active_cnt,
  count(distinct(if(order_type = 'qrcode',sender_id,null))) as his_qr_user_active_cnt

  from 
  opos_temp.opos_active_user_detail_daily
  where 
  country_code='nal'
  group by 
  bd_id,
  city_id
),

--03.13.查出当天活跃的商户,然后计算成功交易次数大于5次的商户所属的bdid所对应的商户个数
active_merchant as (
  select 
  '{pt}' as dt,
  t.bd_id,
  t.city_id,
  count(distinct(t.receipt_id)) as more_5_merchant_cnt

  from 
  --先计算每个商户成功交易的笔数
  (select bd_id,city_id,receipt_id,count(1) as current_complete_cnt from opos_temp.opos_active_user_detail_daily where country_code = 'nal' and dt = '{pt}' group by bd_id,city_id,receipt_id) t 
  where 
  t.current_complete_cnt > 5
  group by
  t.bd_id,
  t.city_id
)


insert overwrite table opos_temp.opos_active_user_daily partition(country_code,dt)
select 
cu.cm_id
,cu.cm_name
,cu.rm_id
,cu.rm_name
,cu.bdm_id
,cu.bdm_name
,cu.bd_id
,cu.bd_name 

,cu.city_id
,cu.city_name
,cu.country

,cu.pos_user_active_cnt
,cu.qr_user_active_cnt
,nvl(dr1.user_active_cnt,0)
,nvl(dr7.user_active_cnt,0)
,nvl(dr15.user_active_cnt,0)
,nvl(dr30.user_active_cnt,0)
,nvl(omd.order_merchant_cnt,0)
,nvl(omd.pos_order_merchant_cnt,0)
,nvl(wd.pos_user_active_cnt,0)
,nvl(wd.qr_user_active_cnt,0)
,nvl(md.pos_user_active_cnt,0)
,nvl(md.qr_user_active_cnt,0)
,nvl(ou.have_order_user_cnt,0)

,cu.user_active_cnt
,cu.new_user_cnt
,nvl(am.more_5_merchant_cnt,0)
,nvl(hd.his_order_merchant_cnt,0)
,nvl(hd.his_pos_order_merchant_cnt,0)
,nvl(hd.his_user_active_cnt,0)
,nvl(hd.his_pos_user_active_cnt,0)
,nvl(hd.his_qr_user_active_cnt,0)

,'nal' as country_code
,'{pt}' as dt

from 
current_user cu
left join day_1_remain dr1 on cu.dt = dr1.dt and cu.bd_id = dr1.bd_id and cu.city_id = dr1.city_id
left join day_7_remain dr7 on cu.dt = dr7.dt and cu.bd_id = dr7.bd_id and cu.city_id = dr7.city_id
left join day_15_remain dr15 on cu.dt = dr15.dt and cu.bd_id = dr15.bd_id and cu.city_id = dr15.city_id
left join day_30_remain dr30 on cu.dt = dr30.dt and cu.bd_id = dr30.bd_id and cu.city_id = dr30.city_id
left join order_merchant_data omd on cu.dt = omd.dt and cu.bd_id = omd.bd_id and cu.city_id = omd.city_id
left join week_data wd on cu.dt = wd.dt and cu.bd_id = wd.bd_id and cu.city_id = wd.city_id
left join month_data md on cu.dt = md.dt and cu.bd_id = md.bd_id and cu.city_id = md.city_id
left join have_order_user ou on cu.dt = ou.dt and cu.bd_id = ou.bd_id and cu.city_id = ou.city_id
left join his_data hd on cu.dt = hd.dt and cu.bd_id = hd.bd_id and cu.city_id = hd.city_id
left join active_merchant am on cu.dt = am.dt and cu.bd_id = am.bd_id and cu.city_id = am.city_id
;


'''.format(
        pt=ds,
        before_1_day=airflow.macros.ds_add(ds, -1),
        before_7_day=airflow.macros.ds_add(ds, -7),
        before_15_day=airflow.macros.ds_add(ds, -15),
        before_30_day=airflow.macros.ds_add(ds, -30),
        table=table_name,
        now_day='{{macros.ds_add(ds, +1)}}',
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = opos_active_user_daily_sql_task(ds)

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


opos_active_user_daily_task = PythonOperator(
    task_id='opos_active_user_daily_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dim_opos_bd_relation_df_task >> opos_active_user_daily_task
ods_sqoop_base_pre_opos_payment_order_di_task >> opos_active_user_daily_task
ods_sqoop_base_pre_opos_payment_order_bd_di_task >> opos_active_user_daily_task

# 查看任务命令
# airflow list_tasks opos_order_metrics_daily -sd /home/feng.yuan/opos_order_metrics_daily.py
# 测试任务命令
# airflow test opos_order_metrics_daily opos_active_user_daily_task 2019-11-25 -sd /home/feng.yuan/opos_order_metrics_daily.py



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

dag = airflow.DAG(
    'opos_order_metrics_daily',
    schedule_interval="10 04 * * *",
    default_args=args)

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
table_name = "opos_active_user_daily"
hdfs_path = "ufile://opay-datalake/oride/opos_temp/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "opos_temp", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


##----------------------------------------- 脚本 ---------------------------------------##

def opos_active_user_daily_sql_task(ds):
    HQL = '''
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;

--03.创建临时明细表,扩充明细信息,将最新一天的交易明细数据关联上dbid和地市编码名称后插入到临时表中
insert overwrite table opos_temp.opos_active_user_detail_daily partition (country_code,dt)
select 
b.cm_id
,b.cm_name
,b.rm_id
,b.rm_name
,b.bdm_id
,b.bdm_name
,s.bd_id
,b.bd_name

,s.city_id
,ci.name as city_name
,ci.country

,p.sender_id
,p.receipt_id
,p.order_type
,p.trade_status
,p.first_order

,'nal' as country_code
,p.dt
from 
(select dt,sender_id,receipt_id,order_id,order_type,trade_status,first_order from opos_dw_ods.ods_sqoop_base_pre_opos_payment_order_di where dt = '{pt}' and trade_status = 'SUCCESS'
) p 
left join
--先用orderod关联每一笔交易的bdid
(
select order_id,bd_id,city_id from opos_dw_ods.ods_sqoop_base_pre_opos_payment_order_bd_di where dt='{pt}'
) as s 
on 
p.order_id = s.order_id
left join
--关联城市码表，求出国家和城市描述
(
select id,name,country from opos_dw_ods.ods_sqoop_base_bd_city_df where dt = '{pt}'
) as ci
on
s.city_id=ci.id
left join
--关联bd信息码表，求出所有bd的层级关系和描述
(
select * from opos_dw.dim_opos_bd_info_df where country_code='nal' and dt='{pt}'
) as b
on
s.bd_id=b.bd_id
where
s.bd_id is not null;

--03.01.查出临时表中昨天,前天,7天前,15天前,30天前的数据
with
active_base as (
  select 
  * 
  from 
  opos_temp.opos_active_user_detail_daily
  where 
  country_code = 'nal' 
  and dt in ('{pt}','{before_1_day}','{before_7_day}','{before_15_day}','{before_30_day}')
),

--03.02.然后从昨天,前天等等特殊历史天数中每个付款客户及对应付款店dbid的次数
user_base as ( 
  select  
  cm_id
  ,cm_name
  ,rm_id
  ,rm_name
  ,bdm_id
  ,bdm_name
  ,bd_id
  ,bd_name 

  ,city_id
  ,city_name
  ,country

  ,sender_id
  ,order_type
  ,sum(if(dt = '{pt}',1,0)) as is_current_day
  ,sum(if(dt ='{before_1_day}',1,0)) as is_before_1_day
  ,sum(if(dt = '{before_7_day}',1,0)) as is_before_7_day
  ,sum(if(dt = '{before_15_day}',1,0)) as is_before_15_day
  ,sum(if(dt = '{before_30_day}',1,0)) as is_before_30_day
  ,sum(if(dt ='{pt}' and first_order = '1',1,0)) as is_new
  from 
  active_base
  group by 
  cm_id
  ,cm_name
  ,rm_id
  ,rm_name
  ,bdm_id
  ,bdm_name
  ,bd_id
  ,bd_name 

  ,city_id
  ,city_name
  ,country

  ,sender_id
  ,order_type
),

--03.03.统计昨日有交易的交易,然后统计每个dbid下的每种订单类型有多数个用户
current_user as (
  select 
  '{pt}' as dt
  ,cm_id
  ,cm_name
  ,rm_id
  ,rm_name
  ,bdm_id
  ,bdm_name
  ,bd_id
  ,bd_name 

  ,city_id
  ,city_name
  ,country

  ,count(distinct sender_id) as user_active_cnt
  ,count(distinct if(order_type = 'pos',sender_id,null)) as pos_user_active_cnt
  ,count(distinct if(order_type = 'qrcode',sender_id,null)) as qr_user_active_cnt
  ,count(distinct if(is_new > 0,sender_id,null)) as new_user_cnt

  from 
  user_base 
  where 
  is_current_day > 0
  group by 
  cm_id
  ,cm_name
  ,rm_id
  ,rm_name
  ,bdm_id
  ,bdm_name
  ,bd_id
  ,bd_name 

  ,city_id
  ,city_name
  ,country
),

--03.04.统计近两天都有交易的客户对应到dbid下的人数
day_1_remain as (

  select 
  '{pt}' as dt,
  bd_id,
  city_id,
  count(distinct sender_id) as user_active_cnt
  from 
  user_base 
  where is_current_day > 0 and is_before_1_day > 0
  group by 
  bd_id,
  city_id
),

--03.05.统计昨天和7天前都有交易的客户对应到dbid下的人数
day_7_remain as (

  select 
  '{pt}' as dt,
  bd_id,
  city_id,
  count(distinct sender_id) as user_active_cnt
  from 
  user_base 
  where is_current_day > 0 and is_before_7_day > 0
  group by 
  bd_id,
  city_id
),

--03.06.统计昨天和15天前都有交易的客户
day_15_remain as (
  select 
  '{pt}' as dt,
  bd_id,
  city_id,
  count(distinct sender_id) as user_active_cnt
  from 
  user_base 
  where is_current_day > 0 and is_before_15_day > 0
  group by 
  bd_id,
  city_id
),

--03.07.统计昨天和30天前都有交易的客户
day_30_remain as (
  select 
  '{pt}' as dt,
  bd_id,
  city_id,
  count(distinct sender_id) as user_active_cnt
  from 
  user_base 
  where is_current_day > 0 and is_before_30_day > 0
  group by 
  bd_id,
  city_id
),

--03.08.统计每个dbid下交易成功的商户数量以及用pos交易的商户数量
order_merchant_data as (
  select 
  '{pt}' as dt,
  bd_id,
  city_id,
  count(distinct(receipt_id)) as order_merchant_cnt,
  count(distinct(if(order_type = 'pos',receipt_id,null))) as pos_order_merchant_cnt
  from 
  active_base
  where dt = '{pt}'
  group by 
  bd_id,
  city_id
),

--03.09.join是inner join取维内自然周的数据
week_data as (
  select 
  '{pt}' as dt,
  u.bd_id,
  u.city_id,
  mt.monday_of_year,
  count(distinct(if(order_type = 'pos',sender_id,null))) as pos_user_active_cnt,
  count(distinct(if(order_type = 'qrcode',sender_id,null))) as qr_user_active_cnt
  from 
  (
  select 
  *
  from 
  opos_temp.opos_active_user_detail_daily 
  where 
  country_code = 'nal' 
  and dt >= '{pt}' 
  and dt <= '{before_15_day}'
  ) u 
  inner join 
  --取出当日所在日期的本周的所有7天的日期
  (select d.dt,d.monday_of_year from public_dw_dim.dim_date d inner join (select * from public_dw_dim.dim_date where dt = '{pt}') t on d.monday_of_year = t.monday_of_year) as mt

  on 
  u.dt = mt.dt
  group by 
  mt.monday_of_year
  ,u.bd_id
  ,u.city_id
),

--03.10.求出每月的dbid分别是pos和qrcode交易的笔数,只拿出所属月的
month_data as (
  select 
  '{pt}' as dt,
  u.bd_id,
  u.city_id,
  mt.month,
  count(distinct(if(order_type = 'pos',sender_id,null))) as pos_user_active_cnt,
  count(distinct(if(order_type = 'qrcode',sender_id,null))) as qr_user_active_cnt

  from 
  (
  select 
  *
  from 
  opos_temp.opos_active_user_detail_daily 
  where country_code = 'nal' 
  and dt <= '{pt}' 
  and dt >= '{before_30_day}'
  ) u 
  inner join 
  --取出当日所在日期的本月的所有的日期
  (select d.dt,d.month from public_dw_dim.dim_date d inner join (select * from public_dw_dim.dim_date where dt = '{pt}') t on d.month = t.month) as mt
  on 
  u.dt = mt.dt
  group by 
  mt.month,
  u.bd_id,
  u.city_id
),

--03.11.取出当天所有付款者的数量
have_order_user as (
  select 
  '{pt}' as dt,
  bd_id,
  city_id,
  count(distinct(sender_id)) as have_order_user_cnt
  from 
  active_base
  where 
  dt = '{pt}'
  group by 
  bd_id,
  city_id
),

--03.12.取出历史数据中所有的付款者，收款者，以及pos和扫码的人数
his_data as (
  select 
  '{pt}' as dt,
  bd_id,
  city_id,
  count(distinct(receipt_id)) as his_order_merchant_cnt,
  count(distinct(if(order_type = 'pos',receipt_id,null))) as his_pos_order_merchant_cnt,

  count(distinct(sender_id)) as his_user_active_cnt,
  count(distinct(if(order_type = 'pos',sender_id,null))) as his_pos_user_active_cnt,
  count(distinct(if(order_type = 'qrcode',sender_id,null))) as his_qr_user_active_cnt

  from 
  opos_temp.opos_active_user_detail_daily
  where 
  country_code='nal'
  group by 
  bd_id,
  city_id
),

--03.13.查出当天活跃的商户,然后计算成功交易次数大于5次的商户所属的bdid所对应的商户个数
active_merchant as (
  select 
  '{pt}' as dt,
  t.bd_id,
  t.city_id,
  count(distinct(t.receipt_id)) as more_5_merchant_cnt

  from 
  --先计算每个商户成功交易的笔数
  (select bd_id,city_id,receipt_id,count(1) as current_complete_cnt from opos_temp.opos_active_user_detail_daily where country_code = 'nal' and dt = '{pt}' group by bd_id,city_id,receipt_id) t 
  where 
  t.current_complete_cnt > 5
  group by
  t.bd_id,
  t.city_id
)


insert overwrite table opos_temp.opos_active_user_daily partition(country_code,dt)
select 
cu.cm_id
,cu.cm_name
,cu.rm_id
,cu.rm_name
,cu.bdm_id
,cu.bdm_name
,cu.bd_id
,cu.bd_name 

,cu.city_id
,cu.city_name
,cu.country

,cu.pos_user_active_cnt
,cu.qr_user_active_cnt
,nvl(dr1.user_active_cnt,0)
,nvl(dr7.user_active_cnt,0)
,nvl(dr15.user_active_cnt,0)
,nvl(dr30.user_active_cnt,0)
,nvl(omd.order_merchant_cnt,0)
,nvl(omd.pos_order_merchant_cnt,0)
,nvl(wd.pos_user_active_cnt,0)
,nvl(wd.qr_user_active_cnt,0)
,nvl(md.pos_user_active_cnt,0)
,nvl(md.qr_user_active_cnt,0)
,nvl(ou.have_order_user_cnt,0)

,cu.user_active_cnt
,cu.new_user_cnt
,nvl(am.more_5_merchant_cnt,0)
,nvl(hd.his_order_merchant_cnt,0)
,nvl(hd.his_pos_order_merchant_cnt,0)
,nvl(hd.his_user_active_cnt,0)
,nvl(hd.his_pos_user_active_cnt,0)
,nvl(hd.his_qr_user_active_cnt,0)

,'nal' as country_code
,'{pt}' as dt

from 
current_user cu
left join day_1_remain dr1 on cu.dt = dr1.dt and cu.bd_id = dr1.bd_id and cu.city_id = dr1.city_id
left join day_7_remain dr7 on cu.dt = dr7.dt and cu.bd_id = dr7.bd_id and cu.city_id = dr7.city_id
left join day_15_remain dr15 on cu.dt = dr15.dt and cu.bd_id = dr15.bd_id and cu.city_id = dr15.city_id
left join day_30_remain dr30 on cu.dt = dr30.dt and cu.bd_id = dr30.bd_id and cu.city_id = dr30.city_id
left join order_merchant_data omd on cu.dt = omd.dt and cu.bd_id = omd.bd_id and cu.city_id = omd.city_id
left join week_data wd on cu.dt = wd.dt and cu.bd_id = wd.bd_id and cu.city_id = wd.city_id
left join month_data md on cu.dt = md.dt and cu.bd_id = md.bd_id and cu.city_id = md.city_id
left join have_order_user ou on cu.dt = ou.dt and cu.bd_id = ou.bd_id and cu.city_id = ou.city_id
left join his_data hd on cu.dt = hd.dt and cu.bd_id = hd.bd_id and cu.city_id = hd.city_id
left join active_merchant am on cu.dt = am.dt and cu.bd_id = am.bd_id and cu.city_id = am.city_id
;


'''.format(
        pt=ds,
        before_1_day=airflow.macros.ds_add(ds, -1),
        before_7_day=airflow.macros.ds_add(ds, -7),
        before_15_day=airflow.macros.ds_add(ds, -15),
        before_30_day=airflow.macros.ds_add(ds, -30),
        table=table_name,
        now_day='{{macros.ds_add(ds, +1)}}',
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = opos_active_user_daily_sql_task(ds)

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


opos_active_user_daily_task = PythonOperator(
    task_id='opos_active_user_daily_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dim_opos_bd_relation_df_task >> opos_active_user_daily_task
ods_sqoop_base_pre_opos_payment_order_di_task >> opos_active_user_daily_task
ods_sqoop_base_pre_opos_payment_order_bd_di_task >> opos_active_user_daily_task

# 查看任务命令
# airflow list_tasks opos_order_metrics_daily -sd /home/feng.yuan/opos_order_metrics_daily.py
# 测试任务命令
# airflow test opos_order_metrics_daily opos_active_user_daily_task 2019-11-25 -sd /home/feng.yuan/opos_order_metrics_daily.py

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

dag = airflow.DAG(
    'opos_order_metrics_daily',
    schedule_interval="10 04 * * *",
    default_args=args)

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
table_name = "opos_active_user_daily"
hdfs_path = "ufile://opay-datalake/oride/opos_temp/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "opos_temp", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


##----------------------------------------- 脚本 ---------------------------------------##

def opos_active_user_daily_sql_task(ds):
    HQL = '''
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;

--03.创建临时明细表,扩充明细信息,将最新一天的交易明细数据关联上dbid和地市编码名称后插入到临时表中
insert overwrite table opos_temp.opos_active_user_detail_daily partition (country_code,dt)
select 
b.cm_id
,b.cm_name
,b.rm_id
,b.rm_name
,b.bdm_id
,b.bdm_name
,s.bd_id
,b.bd_name

,s.city_id
,ci.name as city_name
,ci.country

,p.sender_id
,p.receipt_id
,p.order_type
,p.trade_status
,p.first_order

,'nal' as country_code
,p.dt
from 
(select dt,sender_id,receipt_id,order_id,order_type,trade_status,first_order from opos_dw_ods.ods_sqoop_base_pre_opos_payment_order_di where dt = '{pt}' and trade_status = 'SUCCESS'
) p 
left join
--先用orderod关联每一笔交易的bdid
(
select order_id,bd_id,city_id from opos_dw_ods.ods_sqoop_base_pre_opos_payment_order_bd_di where dt='{pt}'
) as s 
on 
p.order_id = s.order_id
left join
--关联城市码表，求出国家和城市描述
(
select id,name,country from opos_dw_ods.ods_sqoop_base_bd_city_df where dt = '{pt}'
) as ci
on
s.city_id=ci.id
left join
--关联bd信息码表，求出所有bd的层级关系和描述
(
select * from opos_dw.dim_opos_bd_info_df where country_code='nal' and dt='{pt}'
) as b
on
s.bd_id=b.bd_id
where
s.bd_id is not null;

--03.01.查出临时表中昨天,前天,7天前,15天前,30天前的数据
with
active_base as (
  select 
  * 
  from 
  opos_temp.opos_active_user_detail_daily
  where 
  country_code = 'nal' 
  and dt in ('{pt}','{before_1_day}','{before_7_day}','{before_15_day}','{before_30_day}')
),

--03.02.然后从昨天,前天等等特殊历史天数中每个付款客户及对应付款店dbid的次数
user_base as ( 
  select  
  cm_id
  ,cm_name
  ,rm_id
  ,rm_name
  ,bdm_id
  ,bdm_name
  ,bd_id
  ,bd_name 

  ,city_id
  ,city_name
  ,country

  ,sender_id
  ,order_type
  ,sum(if(dt = '{pt}',1,0)) as is_current_day
  ,sum(if(dt ='{before_1_day}',1,0)) as is_before_1_day
  ,sum(if(dt = '{before_7_day}',1,0)) as is_before_7_day
  ,sum(if(dt = '{before_15_day}',1,0)) as is_before_15_day
  ,sum(if(dt = '{before_30_day}',1,0)) as is_before_30_day
  ,sum(if(dt ='{pt}' and first_order = '1',1,0)) as is_new
  from 
  active_base
  group by 
  cm_id
  ,cm_name
  ,rm_id
  ,rm_name
  ,bdm_id
  ,bdm_name
  ,bd_id
  ,bd_name 

  ,city_id
  ,city_name
  ,country

  ,sender_id
  ,order_type
),

--03.03.统计昨日有交易的交易,然后统计每个dbid下的每种订单类型有多数个用户
current_user as (
  select 
  '{pt}' as dt
  ,cm_id
  ,cm_name
  ,rm_id
  ,rm_name
  ,bdm_id
  ,bdm_name
  ,bd_id
  ,bd_name 

  ,city_id
  ,city_name
  ,country

  ,count(distinct sender_id) as user_active_cnt
  ,count(distinct if(order_type = 'pos',sender_id,null)) as pos_user_active_cnt
  ,count(distinct if(order_type = 'qrcode',sender_id,null)) as qr_user_active_cnt
  ,count(distinct if(is_new > 0,sender_id,null)) as new_user_cnt

  from 
  user_base 
  where 
  is_current_day > 0
  group by 
  cm_id
  ,cm_name
  ,rm_id
  ,rm_name
  ,bdm_id
  ,bdm_name
  ,bd_id
  ,bd_name 

  ,city_id
  ,city_name
  ,country
),

--03.04.统计近两天都有交易的客户对应到dbid下的人数
day_1_remain as (

  select 
  '{pt}' as dt,
  bd_id,
  city_id,
  count(distinct sender_id) as user_active_cnt
  from 
  user_base 
  where is_current_day > 0 and is_before_1_day > 0
  group by 
  bd_id,
  city_id
),

--03.05.统计昨天和7天前都有交易的客户对应到dbid下的人数
day_7_remain as (

  select 
  '{pt}' as dt,
  bd_id,
  city_id,
  count(distinct sender_id) as user_active_cnt
  from 
  user_base 
  where is_current_day > 0 and is_before_7_day > 0
  group by 
  bd_id,
  city_id
),

--03.06.统计昨天和15天前都有交易的客户
day_15_remain as (
  select 
  '{pt}' as dt,
  bd_id,
  city_id,
  count(distinct sender_id) as user_active_cnt
  from 
  user_base 
  where is_current_day > 0 and is_before_15_day > 0
  group by 
  bd_id,
  city_id
),

--03.07.统计昨天和30天前都有交易的客户
day_30_remain as (
  select 
  '{pt}' as dt,
  bd_id,
  city_id,
  count(distinct sender_id) as user_active_cnt
  from 
  user_base 
  where is_current_day > 0 and is_before_30_day > 0
  group by 
  bd_id,
  city_id
),

--03.08.统计每个dbid下交易成功的商户数量以及用pos交易的商户数量
order_merchant_data as (
  select 
  '{pt}' as dt,
  bd_id,
  city_id,
  count(distinct(receipt_id)) as order_merchant_cnt,
  count(distinct(if(order_type = 'pos',receipt_id,null))) as pos_order_merchant_cnt
  from 
  active_base
  where dt = '{pt}'
  group by 
  bd_id,
  city_id
),

--03.09.join是inner join取维内自然周的数据
week_data as (
  select 
  '{pt}' as dt,
  u.bd_id,
  u.city_id,
  mt.monday_of_year,
  count(distinct(if(order_type = 'pos',sender_id,null))) as pos_user_active_cnt,
  count(distinct(if(order_type = 'qrcode',sender_id,null))) as qr_user_active_cnt
  from 
  (
  select 
  *
  from 
  opos_temp.opos_active_user_detail_daily 
  where 
  country_code = 'nal' 
  and dt >= '{pt}' 
  and dt <= '{before_15_day}'
  ) u 
  inner join 
  --取出当日所在日期的本周的所有7天的日期
  (select d.dt,d.monday_of_year from public_dw_dim.dim_date d inner join (select * from public_dw_dim.dim_date where dt = '{pt}') t on d.monday_of_year = t.monday_of_year) as mt

  on 
  u.dt = mt.dt
  group by 
  mt.monday_of_year
  ,u.bd_id
  ,u.city_id
),

--03.10.求出每月的dbid分别是pos和qrcode交易的笔数,只拿出所属月的
month_data as (
  select 
  '{pt}' as dt,
  u.bd_id,
  u.city_id,
  mt.month,
  count(distinct(if(order_type = 'pos',sender_id,null))) as pos_user_active_cnt,
  count(distinct(if(order_type = 'qrcode',sender_id,null))) as qr_user_active_cnt

  from 
  (
  select 
  *
  from 
  opos_temp.opos_active_user_detail_daily 
  where country_code = 'nal' 
  and dt <= '{pt}' 
  and dt >= '{before_30_day}'
  ) u 
  inner join 
  --取出当日所在日期的本月的所有的日期
  (select d.dt,d.month from public_dw_dim.dim_date d inner join (select * from public_dw_dim.dim_date where dt = '{pt}') t on d.month = t.month) as mt
  on 
  u.dt = mt.dt
  group by 
  mt.month,
  u.bd_id,
  u.city_id
),

--03.11.取出当天所有付款者的数量
have_order_user as (
  select 
  '{pt}' as dt,
  bd_id,
  city_id,
  count(distinct(sender_id)) as have_order_user_cnt
  from 
  active_base
  where 
  dt = '{pt}'
  group by 
  bd_id,
  city_id
),

--03.12.取出历史数据中所有的付款者，收款者，以及pos和扫码的人数
his_data as (
  select 
  '{pt}' as dt,
  bd_id,
  city_id,
  count(distinct(receipt_id)) as his_order_merchant_cnt,
  count(distinct(if(order_type = 'pos',receipt_id,null))) as his_pos_order_merchant_cnt,

  count(distinct(sender_id)) as his_user_active_cnt,
  count(distinct(if(order_type = 'pos',sender_id,null))) as his_pos_user_active_cnt,
  count(distinct(if(order_type = 'qrcode',sender_id,null))) as his_qr_user_active_cnt

  from 
  opos_temp.opos_active_user_detail_daily
  where 
  country_code='nal'
  group by 
  bd_id,
  city_id
),

--03.13.查出当天活跃的商户,然后计算成功交易次数大于5次的商户所属的bdid所对应的商户个数
active_merchant as (
  select 
  '{pt}' as dt,
  t.bd_id,
  t.city_id,
  count(distinct(t.receipt_id)) as more_5_merchant_cnt

  from 
  --先计算每个商户成功交易的笔数
  (select bd_id,city_id,receipt_id,count(1) as current_complete_cnt from opos_temp.opos_active_user_detail_daily where country_code = 'nal' and dt = '{pt}' group by bd_id,city_id,receipt_id) t 
  where 
  t.current_complete_cnt > 5
  group by
  t.bd_id,
  t.city_id
)


insert overwrite table opos_temp.opos_active_user_daily partition(country_code,dt)
select 
cu.cm_id
,cu.cm_name
,cu.rm_id
,cu.rm_name
,cu.bdm_id
,cu.bdm_name
,cu.bd_id
,cu.bd_name 

,cu.city_id
,cu.city_name
,cu.country

,cu.pos_user_active_cnt
,cu.qr_user_active_cnt
,nvl(dr1.user_active_cnt,0)
,nvl(dr7.user_active_cnt,0)
,nvl(dr15.user_active_cnt,0)
,nvl(dr30.user_active_cnt,0)
,nvl(omd.order_merchant_cnt,0)
,nvl(omd.pos_order_merchant_cnt,0)
,nvl(wd.pos_user_active_cnt,0)
,nvl(wd.qr_user_active_cnt,0)
,nvl(md.pos_user_active_cnt,0)
,nvl(md.qr_user_active_cnt,0)
,nvl(ou.have_order_user_cnt,0)

,cu.user_active_cnt
,cu.new_user_cnt
,nvl(am.more_5_merchant_cnt,0)
,nvl(hd.his_order_merchant_cnt,0)
,nvl(hd.his_pos_order_merchant_cnt,0)
,nvl(hd.his_user_active_cnt,0)
,nvl(hd.his_pos_user_active_cnt,0)
,nvl(hd.his_qr_user_active_cnt,0)

,'nal' as country_code
,'{pt}' as dt

from 
current_user cu
left join day_1_remain dr1 on cu.dt = dr1.dt and cu.bd_id = dr1.bd_id and cu.city_id = dr1.city_id
left join day_7_remain dr7 on cu.dt = dr7.dt and cu.bd_id = dr7.bd_id and cu.city_id = dr7.city_id
left join day_15_remain dr15 on cu.dt = dr15.dt and cu.bd_id = dr15.bd_id and cu.city_id = dr15.city_id
left join day_30_remain dr30 on cu.dt = dr30.dt and cu.bd_id = dr30.bd_id and cu.city_id = dr30.city_id
left join order_merchant_data omd on cu.dt = omd.dt and cu.bd_id = omd.bd_id and cu.city_id = omd.city_id
left join week_data wd on cu.dt = wd.dt and cu.bd_id = wd.bd_id and cu.city_id = wd.city_id
left join month_data md on cu.dt = md.dt and cu.bd_id = md.bd_id and cu.city_id = md.city_id
left join have_order_user ou on cu.dt = ou.dt and cu.bd_id = ou.bd_id and cu.city_id = ou.city_id
left join his_data hd on cu.dt = hd.dt and cu.bd_id = hd.bd_id and cu.city_id = hd.city_id
left join active_merchant am on cu.dt = am.dt and cu.bd_id = am.bd_id and cu.city_id = am.city_id
;


'''.format(
        pt=ds,
        before_1_day=airflow.macros.ds_add(ds, -1),
        before_7_day=airflow.macros.ds_add(ds, -7),
        before_15_day=airflow.macros.ds_add(ds, -15),
        before_30_day=airflow.macros.ds_add(ds, -30),
        table=table_name,
        now_day='{{macros.ds_add(ds, +1)}}',
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = opos_active_user_daily_sql_task(ds)

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


opos_active_user_daily_task = PythonOperator(
    task_id='opos_active_user_daily_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dim_opos_bd_relation_df_task >> opos_active_user_daily_task
ods_sqoop_base_pre_opos_payment_order_di_task >> opos_active_user_daily_task
ods_sqoop_base_pre_opos_payment_order_bd_di_task >> opos_active_user_daily_task

# 查看任务命令
# airflow list_tasks opos_order_metrics_daily -sd /home/feng.yuan/opos_order_metrics_daily.py
# 测试任务命令
# airflow test opos_order_metrics_daily opos_active_user_daily_task 2019-11-25 -sd /home/feng.yuan/opos_order_metrics_daily.py






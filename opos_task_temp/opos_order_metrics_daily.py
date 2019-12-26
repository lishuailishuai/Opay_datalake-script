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
    schedule_interval="00 05 * * *",
    default_args=args)

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

opos_metrcis_report_task = OssSensor(
    task_id='opos_metrcis_report_task',
    bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos/opos_temp/opos_metrcis_report",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name = "opos_temp"
table_name = "opos_active_user_daily"
hdfs_path = "oss://opay-datalake/opos/opos_temp/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "opos_temp", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "6000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


##----------------------------------------- 脚本 ---------------------------------------##

def opos_active_user_daily_sql_task(ds):
    HQL = '''

MSCK REPAIR TABLE opos_dw.dim_opos_bd_info_df;
MSCK REPAIR TABLE opos_dw.dim_opos_bd_relation_df;
MSCK REPAIR TABLE opos_dw.dwd_pre_opos_payment_order_di;
MSCK REPAIR TABLE opos_dw_ods.ods_sqoop_base_bd_city_df;

set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;

--03.01.查出临时表中昨天,前天,7天前,15天前,30天前的数据
with
active_base as (
  select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
  
  ,sender_id
  ,receipt_id
  ,order_type
  ,trade_status
  ,first_order

  ,dt
  from 
  opos_dw.dwd_pre_opos_payment_order_di
  where 
  country_code = 'nal' 
  and dt in ('{pt}','{before_1_day}','{before_7_day}','{before_15_day}','{before_30_day}')
  and trade_status = 'SUCCESS'
),

--03.02.然后从昨天,前天等等特殊历史天数中每个付款客户及对应付款店dbid的次数
user_base as ( 
  select  
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id

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
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id

  ,sender_id
  ,order_type
),

--03.03.统计昨日有交易的交易,然后统计每个dbid下的每种订单类型有多数个用户
current_user as (
  select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id

  ,count(distinct sender_id) as user_active_cnt
  ,count(distinct if(order_type = 'pos',sender_id,null)) as pos_user_active_cnt
  ,count(distinct if(order_type = 'qrcode',sender_id,null)) as qr_user_active_cnt
  ,count(distinct if(is_new > 0,sender_id,null)) as new_user_cnt

  from 
  user_base 
  where 
  is_current_day > 0
  group by 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
),

--03.04.统计近两天都有交易的客户对应到dbid下的人数
day_1_remain as (

  select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
  ,count(distinct sender_id) as user_active_cnt
  from 
  user_base 
  where is_current_day > 0 and is_before_1_day > 0
  group by 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
),

--03.05.统计昨天和7天前都有交易的客户对应到dbid下的人数
day_7_remain as (

  select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
  ,count(distinct sender_id) as user_active_cnt
  from 
  user_base 
  where is_current_day > 0 and is_before_7_day > 0
  group by 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
),

--03.06.统计昨天和15天前都有交易的客户
day_15_remain as (
  select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
  ,count(distinct sender_id) as user_active_cnt
  from 
  user_base 
  where is_current_day > 0 and is_before_15_day > 0
  group by 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
),

--03.07.统计昨天和30天前都有交易的客户
day_30_remain as (
  select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
  ,count(distinct sender_id) as user_active_cnt
  from 
  user_base 
  where is_current_day > 0 and is_before_30_day > 0
  group by 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
),

--03.08.统计每个dbid下交易成功的商户数量以及用pos交易的商户数量
order_merchant_data as (
  select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
  ,count(distinct(receipt_id)) as order_merchant_cnt
  ,count(distinct(if(order_type = 'pos',receipt_id,null))) as pos_order_merchant_cnt
  from 
  active_base
  where dt = '{pt}'
  group by 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
),

--03.09.join是inner join取维内自然周的数据
week_data as (
  select 
  u.hcm_id
  ,u.cm_id
  ,u.rm_id
  ,u.bdm_id
  ,u.bd_id

  ,u.city_id
  ,mt.monday_of_year
  ,count(distinct(if(u.order_type = 'pos',u.sender_id,null))) as pos_user_active_cnt
  ,count(distinct(if(u.order_type = 'qrcode',u.sender_id,null))) as qr_user_active_cnt
  from 
    (
    select 
    hcm_id
    ,cm_id
    ,rm_id
    ,bdm_id
    ,bd_id
    ,city_id
    ,order_type
    ,sender_id
    ,dt
    from 
    opos_dw.dwd_pre_opos_payment_order_di 
    where 
    country_code = 'nal' 
    and dt <= '{pt}' 
    and dt >= '{before_7_day}'
    and trade_status = 'SUCCESS'
    ) u 
  inner join 
  --取出当日所在日期的本周的所有7天的日期
    (select d.dt,d.monday_of_year from public_dw_dim.dim_date d inner join (select * from public_dw_dim.dim_date where dt = '{pt}') t on d.monday_of_year = t.monday_of_year) as mt
  on u.dt = mt.dt
  group by 
  u.hcm_id
  ,u.cm_id
  ,u.rm_id
  ,u.bdm_id
  ,u.bd_id

  ,u.city_id
  ,mt.monday_of_year
),

--03.10.求出每月的dbid分别是pos和qrcode交易的笔数,只拿出所属月的
month_data as (
  select 
  u.hcm_id
  ,u.cm_id
  ,u.rm_id
  ,u.bdm_id
  ,u.bd_id

  ,u.city_id
  ,mt.month
  ,count(distinct(if(u.order_type = 'pos',u.sender_id,null))) as pos_user_active_cnt
  ,count(distinct(if(u.order_type = 'qrcode',u.sender_id,null))) as qr_user_active_cnt
  from 
    (
    select 
    hcm_id
    ,cm_id
    ,rm_id
    ,bdm_id
    ,bd_id
    ,city_id
    ,order_type
    ,sender_id
    ,dt
    from 
    opos_dw.dwd_pre_opos_payment_order_di 
    where 
    country_code = 'nal' 
    and dt <= '{pt}' 
    and dt >= '{before_30_day}'
    and trade_status = 'SUCCESS'
    ) u 
  inner join 
  --取出当日所在日期的本月的所有的日期
    (select d.dt,d.month from public_dw_dim.dim_date d inner join (select * from public_dw_dim.dim_date where dt = '{pt}') t on d.month = t.month) as mt
  on u.dt = mt.dt
  group by 
  u.hcm_id
  ,u.cm_id
  ,u.rm_id
  ,u.bdm_id
  ,u.bd_id

  ,u.city_id
  ,mt.month
),

--03.11.取出当天所有付款者的数量
have_order_user as (
  select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
  ,count(distinct(sender_id)) as have_order_user_cnt
  from 
  active_base
  where 
  dt = '{pt}'
  group by 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
),

--03.12.取出历史数据中所有的付款者，收款者，以及pos和扫码的人数
his_data as (
  select 
  '{pt}' as dt
  ,hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
  ,count(distinct(receipt_id)) as his_order_merchant_cnt
  ,count(distinct(if(order_type = 'pos',receipt_id,null))) as his_pos_order_merchant_cnt

  ,count(distinct(sender_id)) as his_user_active_cnt
  ,count(distinct(if(order_type = 'pos',sender_id,null))) as his_pos_user_active_cnt
  ,count(distinct(if(order_type = 'qrcode',sender_id,null))) as his_qr_user_active_cnt
  from  

    (
    select 
    hcm_id
    ,cm_id
    ,rm_id
    ,bdm_id
    ,bd_id
  
    ,city_id
    ,order_type
    ,receipt_id
    ,sender_id
    from  
    opos_dw.dwd_pre_opos_payment_order_di 
    where 
    country_code = 'nal' 
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
    ,order_type
    ,receipt_id
    ,sender_id
    ) as m

  group by 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
),

--03.13.查出当天活跃的商户,然后计算成功交易次数大于5次的商户所属的bdid所对应的商户个数
active_merchant as (
  select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
  ,count(distinct(t.receipt_id)) as more_5_merchant_cnt

  from 
  --先计算每个商户成功交易的笔数
    (
    select 
    hcm_id
    ,cm_id
    ,rm_id
    ,bdm_id
    ,bd_id
  
    ,city_id
    ,receipt_id
    ,count(1) as current_complete_cnt 
    from 
    opos_dw.dwd_pre_opos_payment_order_di 
    where 
    country_code = 'nal' 
    and dt = '{pt}' 
    and trade_status = 'SUCCESS'
    group by 
    hcm_id
    ,cm_id
    ,rm_id
    ,bdm_id
    ,bd_id
  
    ,city_id
    ,receipt_id) t 
  where 
  current_complete_cnt > 5
  group by
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
)

insert overwrite table opos_temp.opos_active_user_daily partition(country_code,dt)
select 
cu.hcm_id
,cu.cm_id
,cu.rm_id
,cu.bdm_id
,cu.bd_id

,cu.city_id

,d.dt as create_date
,d.week_of_year as create_week
,substr(d.dt,0,7) as create_month
,substr(d.dt,0,4) as create_year

,nvl(hd.pos_user_active_cnt,0) pos_user_active_cnt
,nvl(hd.qr_user_active_cnt,0) qr_user_active_cnt
,nvl(dr1.user_active_cnt,0) before_1_day_user_active_cnt
,nvl(dr7.user_active_cnt,0) before_7_day_user_active_cnt
,nvl(dr15.user_active_cnt,0) before_15_day_user_active_cnt
,nvl(dr30.user_active_cnt,0) before_30_day_user_active_cnt
,nvl(omd.order_merchant_cnt,0) order_merchant_cnt
,nvl(omd.pos_order_merchant_cnt,0) pos_order_merchant_cnt
,nvl(wd.pos_user_active_cnt,0) week_pos_user_active_cnt
,nvl(wd.qr_user_active_cnt,0) week_qr_user_active_cnt
,nvl(md.pos_user_active_cnt,0) month_pos_user_active_cnt
,nvl(md.qr_user_active_cnt,0) month_qr_user_active_cnt
,nvl(ou.have_order_user_cnt,0) have_order_user_cnt

,nvl(hd.user_active_cnt,0) user_active_cnt
,nvl(hd.new_user_cnt,0) new_user_cnt
,nvl(am.more_5_merchant_cnt,0) more_5_merchant_cnt
,nvl(cu.his_order_merchant_cnt,0) his_order_merchant_cnt
,nvl(cu.his_pos_order_merchant_cnt,0) his_pos_order_merchant_cnt
,nvl(cu.his_user_active_cnt,0) his_user_active_cnt
,nvl(cu.his_pos_user_active_cnt,0) his_pos_user_active_cnt
,nvl(cu.his_qr_user_active_cnt,0) his_qr_user_active_cnt

,'nal' as country_code
,'{pt}' as dt

from 
his_data cu
left join 
day_1_remain dr1 
on cu.hcm_id = dr1.hcm_id and cu.cm_id = dr1.cm_id and cu.rm_id = dr1.rm_id and cu.bdm_id = dr1.bdm_id and cu.bd_id = dr1.bd_id and cu.city_id = dr1.city_id
left join 
day_7_remain dr7 
on cu.hcm_id = dr7.hcm_id and cu.cm_id = dr7.cm_id and cu.rm_id = dr7.rm_id and cu.bdm_id = dr7.bdm_id and cu.bd_id = dr7.bd_id and cu.city_id = dr7.city_id
left join 
day_15_remain dr15 
on cu.hcm_id = dr15.hcm_id and cu.cm_id = dr15.cm_id and cu.rm_id = dr15.rm_id and cu.bdm_id = dr15.bdm_id and cu.bd_id = dr15.bd_id and cu.city_id = dr15.city_id
left join 
day_30_remain dr30 
on cu.hcm_id = dr30.hcm_id and cu.cm_id = dr30.cm_id and cu.rm_id = dr30.rm_id and cu.bdm_id = dr30.bdm_id and cu.bd_id = dr30.bd_id and cu.city_id = dr30.city_id
left join 
order_merchant_data omd 
on cu.hcm_id = omd.hcm_id and cu.cm_id = omd.cm_id and cu.rm_id = omd.rm_id and cu.bdm_id = omd.bdm_id and cu.bd_id = omd.bd_id and cu.city_id = omd.city_id
left join 
week_data wd 
on cu.hcm_id = wd.hcm_id and cu.cm_id = wd.cm_id and cu.rm_id = wd.rm_id and cu.bdm_id = wd.bdm_id and cu.bd_id = wd.bd_id and cu.city_id = wd.city_id
left join 
month_data md 
on cu.hcm_id = md.hcm_id and cu.cm_id = md.cm_id and cu.rm_id = md.rm_id and cu.bdm_id = md.bdm_id and cu.bd_id = md.bd_id and cu.city_id = md.city_id
left join 
have_order_user ou 
on cu.hcm_id = ou.hcm_id and cu.cm_id = ou.cm_id and cu.rm_id = ou.rm_id and cu.bdm_id = ou.bdm_id and cu.bd_id = ou.bd_id and cu.city_id = ou.city_id
left join 
current_user hd 
on cu.hcm_id = hd.hcm_id and cu.cm_id = hd.cm_id and cu.rm_id = hd.rm_id and cu.bdm_id = hd.bdm_id and cu.bd_id = hd.bd_id and cu.city_id = hd.city_id
left join 
active_merchant am 
on cu.hcm_id = am.hcm_id and cu.cm_id = am.cm_id and cu.rm_id = am.rm_id and cu.bdm_id = am.bdm_id and cu.bd_id = am.bd_id and cu.city_id = am.city_id
left join
(select dt,week_of_year from public_dw_dim.dim_date where dt='{pt}') as d
on cu.dt=d.dt
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

# 将数据推到mysql一份
delete_crm_data = MySqlOperator(
    task_id='delete_crm_data',
    sql="""
        DELETE FROM opos_metrics_daily WHERE dt='{ds}' ;
    """.format(
        ds='{{ds}}'
    ),
    mysql_conn_id='mysql_dw',
    dag=dag)

delete_bi_data = MySqlOperator(
    task_id='delete_bi_data',
    sql="""
        DELETE FROM opos_metrics_daily WHERE dt='{ds}' ;
    """.format(
        ds='{{ds}}'
    ),
    mysql_conn_id='mysql_bi',
    dag=dag)

delete_bi_bd_data = MySqlOperator(
    task_id='delete_bi_bd_data',
    sql="""
        DELETE FROM opos_metrics_daily_d WHERE dt='{ds}' ;
    """.format(
        ds='{{ds}}'
    ),
    mysql_conn_id='mysql_bi',
    dag=dag)

insert_crm_metrics = HiveToMySqlTransfer(
    task_id='insert_crm_metrics',
    sql=""" 
        
 select 
        null,
        a.dt,
        cast(a.bd_id as string) as bd_id,
        if(a.city_id='-',0,a.city_id) as city_id,
        a.merchant_cnt , 
        a.pos_merchant_cnt , 
        a.new_merchant_cnt , 
        a.new_pos_merchant_cnt , 
        a.pos_complete_order_cnt , 
        a.qr_complete_order_cnt , 
        a.complete_order_cnt , 
        a.gmv ,
        a.actual_amount ,
        nvl(b.pos_user_active_cnt,0),
        nvl(b.qr_user_active_cnt,0),
        nvl(b.before_1_day_user_active_cnt,0),
        nvl(b.before_7_day_user_active_cnt,0),
        nvl(b.before_15_day_user_active_cnt,0),
        nvl(b.before_30_day_user_active_cnt,0),
        nvl(b.order_merchant_cnt,0),
        nvl(b.pos_order_merchant_cnt,0),
        nvl(b.week_pos_user_active_cnt,0),
        nvl(b.week_qr_user_active_cnt,0),
        nvl(b.month_pos_user_active_cnt,0),
        nvl(b.month_qr_user_active_cnt,0),
        nvl(b.have_order_user_cnt,0),
        nvl(c.name,'-') as city_name,
        nvl(c.country,'-') as country,

        a.return_amount,
        a.new_user_cost,
        a.old_user_cost,
        a.return_amount_order_cnt,
        a.his_pos_complete_order_cnt,
        a.his_qr_complete_order_cnt,
        a.his_complete_order_cnt,
        a.his_gmv,
        a.his_actual_amount,
        a.his_return_amount,
        a.his_new_user_cost,
        a.his_old_user_cost,
        a.his_return_amount_order_cnt,
        nvl(b.user_active_cnt,0),
        nvl(b.new_user_cnt,0),
        nvl(b.more_5_merchant_cnt,0),
        nvl(b.his_order_merchant_cnt,0),
        nvl(b.his_pos_order_merchant_cnt,0),
        nvl(b.his_user_active_cnt,0),
        nvl(b.his_pos_user_active_cnt,0),
        nvl(b.his_qr_user_active_cnt,0)
        from 
        (select * from opos_temp.opos_metrcis_report where country_code = 'nal' and  dt = '{{ ds }}') a
        left join 
        (select * from opos_temp.opos_active_user_daily where country_code = 'nal' and  dt = '{{ ds }}') b 
        on  
a.hcm_id=b.hcm_id
AND a.cm_id=b.cm_id
AND a.rm_id=b.rm_id
AND a.bdm_id=b.bdm_id
AND a.bd_id=b.bd_id
and a.city_id=b.city_id   
        left join
  (select id,name,country from opos_dw_ods.ods_sqoop_base_bd_city_df where dt = '{{ ds }}') as c
on
  a.city_id=c.id
   

        """,
    mysql_conn_id='mysql_dw',
    mysql_table='opos_metrics_daily',
    dag=dag)

insert_bi_metrics = HiveToMySqlTransfer(
    task_id='insert_bi_metrics',
    sql=""" 
           
 select 
        null,
        a.dt,
        cast(a.bd_id as string) as bd_id,
        if(a.city_id='-',0,a.city_id) as city_id,
        a.merchant_cnt , 
        a.pos_merchant_cnt , 
        a.new_merchant_cnt , 
        a.new_pos_merchant_cnt , 
        a.pos_complete_order_cnt , 
        a.qr_complete_order_cnt , 
        a.complete_order_cnt , 
        a.gmv ,
        a.actual_amount ,
        nvl(b.pos_user_active_cnt,0),
        nvl(b.qr_user_active_cnt,0),
        nvl(b.before_1_day_user_active_cnt,0),
        nvl(b.before_7_day_user_active_cnt,0),
        nvl(b.before_15_day_user_active_cnt,0),
        nvl(b.before_30_day_user_active_cnt,0),
        nvl(b.order_merchant_cnt,0),
        nvl(b.pos_order_merchant_cnt,0),
        nvl(b.week_pos_user_active_cnt,0),
        nvl(b.week_qr_user_active_cnt,0),
        nvl(b.month_pos_user_active_cnt,0),
        nvl(b.month_qr_user_active_cnt,0),
        nvl(b.have_order_user_cnt,0),
        nvl(c.name,'-') as city_name,
        nvl(c.country,'-') as country,

        a.return_amount,
        a.new_user_cost,
        a.old_user_cost,
        a.return_amount_order_cnt,
        a.his_pos_complete_order_cnt,
        a.his_qr_complete_order_cnt,
        a.his_complete_order_cnt,
        a.his_gmv,
        a.his_actual_amount,
        a.his_return_amount,
        a.his_new_user_cost,
        a.his_old_user_cost,
        a.his_return_amount_order_cnt,
        nvl(b.user_active_cnt,0),
        nvl(b.new_user_cnt,0),
        nvl(b.more_5_merchant_cnt,0),
        nvl(b.his_order_merchant_cnt,0),
        nvl(b.his_pos_order_merchant_cnt,0),
        nvl(b.his_user_active_cnt,0),
        nvl(b.his_pos_user_active_cnt,0),
        nvl(b.his_qr_user_active_cnt,0)
        from 
        (select * from opos_temp.opos_metrcis_report where country_code = 'nal' and  dt = '{{ ds }}') a
        left join 
        (select * from opos_temp.opos_active_user_daily where country_code = 'nal' and  dt = '{{ ds }}') b 
        on  
a.hcm_id=b.hcm_id
AND a.cm_id=b.cm_id
AND a.rm_id=b.rm_id
AND a.bdm_id=b.bdm_id
AND a.bd_id=b.bd_id
and a.city_id=b.city_id   
        left join
  (select id,name,country from opos_dw_ods.ods_sqoop_base_bd_city_df where dt = '{{ ds }}') as c
on
  a.city_id=c.id
   

    """,
    mysql_conn_id='mysql_bi',
    mysql_table='opos_metrics_daily',
    dag=dag)

insert_bi_bd_metrics = HiveToMySqlTransfer(
    task_id='insert_bi_bd_metrics',
    sql=""" 
    
    

        select 
        null,
        a.dt,
        
        d.week_of_year as create_week,
        substr(a.dt,0,7) as create_month,
        substr(a.dt,0,4) as create_year,

        cast(a.cm_id as string) as cm_id,
        nvl(cm.name,'-') as cm_name,
        cast(a.rm_id as string) as rm_id,
        nvl(rm.name,'-') as rm_name,
        cast(a.bdm_id as string) as bdm_id,
        nvl(bdm.name,'-') as bdm_name,
        cast(a.bd_id as string) as bd_id,
        nvl(bd.name,'-') as bd_name,

        if(a.city_id='-',0,a.city_id) as city_id,
        nvl(c.name,'-') as city_name,
        nvl(c.country,'-') as country,

        a.merchant_cnt , 
        a.pos_merchant_cnt , 
        a.new_merchant_cnt , 
        a.new_pos_merchant_cnt , 
        a.pos_complete_order_cnt , 
        a.qr_complete_order_cnt , 
        a.complete_order_cnt , 
        a.gmv ,
        a.actual_amount ,
        nvl(b.pos_user_active_cnt,0),
        nvl(b.qr_user_active_cnt,0),
        nvl(b.before_1_day_user_active_cnt,0),
        nvl(b.before_7_day_user_active_cnt,0),
        nvl(b.before_15_day_user_active_cnt,0),
        nvl(b.before_30_day_user_active_cnt,0),
        nvl(b.order_merchant_cnt,0),
        nvl(b.pos_order_merchant_cnt,0),
        nvl(b.week_pos_user_active_cnt,0),
        nvl(b.week_qr_user_active_cnt,0),
        nvl(b.month_pos_user_active_cnt,0),
        nvl(b.month_qr_user_active_cnt,0),
        nvl(b.have_order_user_cnt,0),

        a.return_amount,
        a.new_user_cost,
        a.old_user_cost,
        a.return_amount_order_cnt,
        a.his_pos_complete_order_cnt,
        a.his_qr_complete_order_cnt,
        a.his_complete_order_cnt,
        a.his_gmv,
        a.his_actual_amount,
        a.his_return_amount,
        a.his_new_user_cost,
        a.his_old_user_cost,
        a.his_return_amount_order_cnt,
        nvl(b.user_active_cnt,0),
        nvl(b.new_user_cnt,0),
        nvl(b.more_5_merchant_cnt,0),
        nvl(b.his_order_merchant_cnt,0),
        nvl(b.his_pos_order_merchant_cnt,0),
        nvl(b.his_user_active_cnt,0),
        nvl(b.his_pos_user_active_cnt,0),
        nvl(b.his_qr_user_active_cnt,0)
        from 
        (select * from opos_temp.opos_metrcis_report where country_code = 'nal' and  dt = '{{ ds }}') a
        left join 
        (select * from opos_temp.opos_active_user_daily where country_code = 'nal' and  dt = '{{ ds }}') b 
        on  
a.hcm_id=b.hcm_id
AND a.cm_id=b.cm_id
AND a.rm_id=b.rm_id
AND a.bdm_id=b.bdm_id
AND a.bd_id=b.bd_id
and a.city_id=b.city_id
        left join
        public_dw_dim.dim_date as d
        on
        a.dt=d.dt

        left join
    (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{{ ds }}') as bd
  on a.bd_id=bd.id
  left join
    (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{{ ds }}') as bdm
  on a.bdm_id=bdm.id
  left join
    (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{{ ds }}') as rm
  on a.rm_id=rm.id
  left join
    (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{{ ds }}') as cm
  on a.cm_id=cm.id
  left join
    (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{{ ds }}') as hcm
  on a.hcm_id=hcm.id
left join
  (select id,name,country from opos_dw_ods.ods_sqoop_base_bd_city_df where dt = '{{ ds }}') as c
on
  a.city_id=c.id



    """,
    mysql_conn_id='mysql_bi',
    mysql_table='opos_metrics_daily_d',
    dag=dag)

dwd_pre_opos_payment_order_di_task >> opos_active_user_daily_task

# 执行mysql任务
opos_active_user_daily_task >> delete_crm_data >> insert_crm_metrics >> delete_bi_data >> insert_bi_metrics >> delete_bi_bd_data >> insert_bi_bd_metrics
opos_metrcis_report_task >> delete_crm_data >> insert_crm_metrics >> delete_bi_data >> insert_bi_metrics >> delete_bi_bd_data >> insert_bi_bd_metrics

# 查看任务命令
# airflow list_tasks opos_order_metrics_daily -sd /home/feng.yuan/opos_order_metrics_daily.py
# 测试任务命令
# airflow test opos_order_metrics_daily opos_active_user_daily_task 2019-11-26 -sd /home/feng.yuan/opos_order_metrics_daily.py



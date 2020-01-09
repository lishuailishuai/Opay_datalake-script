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
from plugins.CountriesPublicFrame import CountriesPublicFrame
import json
import logging
from airflow.models import Variable
from airflow.sensors import OssSensor
import requests
import os

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2019, 11, 18),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_global_operate_report_multi_d',
                  schedule_interval="00 01 * * *",
                  default_args=args)


##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_global_operate_report_multi_d"

##----------------------------------------- 依赖 ---------------------------------------##

#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":

    # 依赖前一天分区
    dependence_dm_oride_passenger_base_multi_cube_prev_day_task = UFileSensor(
        task_id='dm_oride_passenger_base_multi_cube_prev_day_task',
        filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dm_oride_passenger_base_multi_cube/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    
    dependence_dm_oride_order_base_d_prev_day_task = UFileSensor(
        task_id='dm_oride_order_base_d_prev_day_task',
        filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dm_oride_order_base_d/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

else:

    # 依赖前一天分区
    dependence_dm_oride_passenger_base_multi_cube_prev_day_task = OssSensor(
        task_id='dm_oride_passenger_base_multi_cube_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dm_oride_passenger_base_multi_cube/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    
    dependence_dm_oride_order_base_d_prev_day_task = OssSensor(
        task_id='dm_oride_order_base_d_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dm_oride_order_base_d/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name



##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def app_oride_global_operate_report_multi_d_sql_task(ds):
    HQL ='''
    SET hive.exec.parallel=true;
    SET hive.exec.dynamic.partition=true;
    SET hive.exec.dynamic.partition.mode=nonstrict;
    set hive.auto.convert.join = false;

    with order_data as 
 (SELECT dt,
       if(dt<'2019-12-08' and country_code='nal','NG',country_code) as country_code,
       city_id,
       driver_serv_type, --订单表司机业务类型
       sum(ride_order_cnt) as ride_order_cnt, --当日下单量
       sum(valid_ord_cnt) as valid_ord_cnt,  --当日有效订单量
       sum(finish_order_cnt) as finish_order_cnt, --当日完单量
       sum(finish_pay) as finish_pay, --当日成功完成支付量，1218号逻辑升级      
       sum(wet_ord_cnt) as wet_ord_cnt, --当日湿单订单量
       order_cnt_lfw, --近四周同期下单数据均值
       finish_order_cnt_lfw  --近四周同期完单数据

FROM (SELECT sum(if(dt>=date_add('{pt}',-28)
              AND dt<'{pt}'
              AND from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')=from_unixtime(unix_timestamp('{pt}', 'yyyy-MM-dd'),'u'),ride_order_cnt,0)) over (partition BY '{pt}',country_code_new, city_id, driver_serv_type)/4 AS order_cnt_lfw,--近四周同期下单数据均值
       sum(if(dt>=date_add('{pt}',-28)
              AND dt<'{pt}'
              AND from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')=from_unixtime(unix_timestamp('{pt}', 'yyyy-MM-dd'),'u'),finish_order_cnt,0)) over (partition BY '{pt}',country_code_new, city_id, driver_serv_type)/4 AS finish_order_cnt_lfw,--近四周同期完单数据
       *
from (select *,if(dt<'2019-12-08' and country_code='nal','NG',country_code) as country_code_new  --此处由于新上线国家码，因此需要处理下统计近四周数据
FROM oride_dw.dm_oride_order_base_d
WHERE dt>=date_add('{pt}',-28)
  AND dt<='{pt}') t) m 
  where m.dt='{pt}'
  group by m.dt,if(dt<'2019-12-08' and country_code='nal','NG',country_code),
             m.city_id,
             m.driver_serv_type,
             m.order_cnt_lfw,
             m.finish_order_cnt_lfw)

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)       
select nvl(city_id,-10000) as city_id,
       nvl(driver_serv_type,-10000) as driver_serv_type,
       sum(ride_order_cnt) as ride_order_cnt, --当日下单量
       sum(valid_ord_cnt) as valid_ord_cnt,  --当日有效订单量
       sum(finish_order_cnt) as finish_order_cnt, --当日完单量
       sum(finish_pay) as finish_pay, --当日完成支付量       
       sum(wet_ord_cnt) as wet_ord_cnt, --当日湿单订单量
       sum(order_cnt_lfw) as order_cnt_lfw,  --近四周同期下单数据 
       sum(finish_order_cnt_lfw) as finish_order_cnt_lfw,  --近四周同期完单数据
       sum(ord_users) as ord_users,  --当日下单乘客数
       sum(finished_users) as finished_users,  --当日完单乘客数
       sum(nobeckon_paid_users) as nobeckon_paid_users,  --当日总支付乘客数，自12.18号升级逻辑去掉招手停，发邮件需要改字段名称
       sum(nobeckon_opay_paid_users) as nobeckon_opay_paid_users,  --当日opay支付乘客数，自12.18号升级逻辑去掉招手停，发邮件需要改字段名称
       sum(nobeckon_online_paid_users) as nobeckon_online_paid_users,  --当日线上支付乘客数，自12.18号新增
       --nvl(country_code,'total') as country_code,
       if(nvl(country_code,'total')='total','nal','nal') as country_code,
       '{pt}' as dt
        
from (select nvl(ord.country_code,'total') as country_code,
       city_id,
       driver_serv_type,
       sum(ride_order_cnt) as ride_order_cnt, --当日下单量
       sum(valid_ord_cnt) as valid_ord_cnt,  --当日有效订单量
       sum(finish_order_cnt) as finish_order_cnt, --当日完单量
       sum(finish_pay) as finish_pay, --当日成功完成支付量，自1218号开始逻辑升级       
       sum(wet_ord_cnt) as wet_ord_cnt, --当日湿单订单量
       sum(order_cnt_lfw) as order_cnt_lfw,  --近四周同期下单数据 
       sum(finish_order_cnt_lfw) as finish_order_cnt_lfw,  --近四周同期完单数据
       null as ord_users,  --当日下单乘客数
       null as finished_users,  --当日完单乘客数
       null as nobeckon_paid_users,  --当日总支付乘客数
       null as nobeckon_opay_paid_users,  --当日opay支付乘客数
       null as nobeckon_online_paid_users  --当日线上支付乘客数
from order_data ord
inner join
(select city_id
from oride_dw.dim_oride_city
where dt='{pt}' and size(split(product_id,','))>1) multi_cit
on ord.city_id=multi_cit.city_id
group by nvl(ord.country_code,'total'),
       ord.city_id,
       ord.driver_serv_type
with cube

union all

select country_code,
       city_id,
       driver_serv_type,
       null as ride_order_cnt, --当日下单量
       null as valid_ord_cnt,  --当日有效订单量
       null as finish_order_cnt, --当日完单量
       null as finish_pay, --当日完成支付量       
       null as wet_ord_cnt, --当日湿单订单量
       null as order_cnt_lfw,  --近四周同期下单数据 
       null as finish_order_cnt_lfw,  --近四周同期完单数据
       ord_users,  --当日下单乘客数，有同时呼叫时，该指标在driver_serv_type维度下的下单乘客数是不准确的，由于多业务线报表只看城市维度  
       finished_users,  --当日完单乘客数
       nobeckon_paid_users,  --当日总支付乘客数
       nobeckon_opay_paid_users,  --当日opay支付乘客数
       nobeckon_online_paid_users  --当日线上支付乘客数
from oride_dw.dm_oride_passenger_base_multi_cube
where dt='{pt}' and product_id=-10000) m
where nvl(m.country_code,'total')='total'
group by if(nvl(country_code,'total')='total','nal','nal'),
       nvl(city_id,-10000),
       nvl(driver_serv_type,-10000);
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )
    return HQL

# 主流程
def execution_data_task_id(ds, **kwargs):
    v_date = kwargs.get('v_execution_date')
    v_day = kwargs.get('v_execution_day')
    v_hour = kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()
    """
            #功能函数
            alter语句: alter_partition
            删除分区: delete_partition
            生产success: touchz_success

            #参数
            第一个参数true: 所有国家是否上线。false 没有
            第二个参数true: 数据目录是有country_code分区。false 没有
            第三个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

            #读取sql
            %_sql(ds,v_hour)

            第一个参数ds: 天级任务
            第二个参数v_hour: 小时级任务，需要使用

        """
    cf = CountriesPublicFrame("false", ds, db_name, table_name, hdfs_path, "true", "true")

    # 删除分区
    #cf.delete_partition()

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + app_oride_global_operate_report_multi_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 生产success
    cf.touchz_success()

app_oride_global_operate_report_multi_d_task = PythonOperator(
    task_id='app_oride_global_operate_report_multi_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

dependence_dm_oride_passenger_base_multi_cube_prev_day_task >> app_oride_global_operate_report_multi_d_task
dependence_dm_oride_order_base_d_prev_day_task >> app_oride_global_operate_report_multi_d_task

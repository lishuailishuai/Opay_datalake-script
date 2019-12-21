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
    'owner': 'lijialong',
    'start_date': datetime(2019, 9, 9),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_otrike_order_carpool_d',
                  schedule_interval="50 03 * * *",
                  default_args=args)

##----------------------------------------- 依赖 ---------------------------------------##


# 依赖前一天分区
dependence_dm_oride_order_base_d_prev_day_task = UFileSensor(
    task_id='dm_oride_order_base_d_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dm_oride_order_base_d/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dim_oride_city_task = HivePartitionSensor(
    task_id="dim_oride_city_task",
    table="dim_oride_city",
    partition="dt='{{ ds }}'",
    schema="oride_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)



##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name="oride_dw"
table_name = "app_oride_otrike_order_carpool_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##


def app_oride_otrike_order_carpool_d_sql_task(ds):

    HQL='''
    SET hive.exec.parallel=true;
    SET hive.exec.dynamic.partition=true;
    SET hive.exec.dynamic.partition.mode=nonstrict;

    insert overwrite TABLE oride_dw.{table} partition(country_code,dt)
    select
    --d日数据
    d.city_id, --城市id
    d.city_name,--城市名称
    d.carpool_order_num_d,--日拼车订单数
    d.otrike_order_num_d,--日成功发起otrike订单数，即拼车订单+包车订单
    d.carpool_success_order_num_d,--日拼成的订单数量
    d.carpool_accept_order_num_d,--日拼单池内被应答的订单数量
    d.carpool_success_and_finish_order_num_d,--日拼车成功且完成的order数量
    
    --w周数据
    w.carpool_order_num_w,--周拼车订单数
    w.otrike_order_num_w,--周成功发起otrike订单数，即拼车订单+包车订单
    w.carpool_success_order_num_w,--周拼成的订单数量
    w.carpool_accept_order_num_w,--周拼单池内被应答的订单数量
    w.carpool_success_and_finish_order_num_w,--周拼车成功且完成的order数量
    w.week_num, --自然周
    
    --m月数据
    m.carpool_order_num_m,--月拼车订单数
    m.otrike_order_num_m,--月成功发起otrike订单数，即拼车订单+包车订单
    m.carpool_success_order_num_m,--月拼成的订单数量
    m.carpool_accept_order_num_m,--月拼单池内被应答的订单数量
    m.carpool_success_and_finish_order_num_m,--月拼车成功且完成的order数量
    m.month_num,--自然月
    d.country_code,
    d.dt
    
from 
(
    --城市日数据
    select
        ob.city_id as city_id,
        oc.city_name as city_name,
        ob.carpool_order_num as carpool_order_num_d,
        ob.otrike_order_num as otrike_order_num_d,
        ob.carpool_success_order_num as carpool_success_order_num_d,
        ob.carpool_accept_order_num as carpool_accept_order_num_d,
        ob.carpool_success_and_finish_order_num as carpool_success_and_finish_order_num_d,
        ob.country_code as country_code,
        ob.dt as dt
    from 
    (
        select 
            city_id,
            product_id,
            nvl(sum(carpool_num),0) as carpool_order_num, --拼车订单数
            --sum(chartered_bus_num), --包车
            --nvl(round(sum(carpool_num) / (sum(carpool_num)+sum(chartered_bus_num)),2),0)  as willing_to_carpool_rate_d,--愿拼率：拼车 / 拼车+包车
            nvl(sum(carpool_num)+sum(chartered_bus_num),0) as otrike_order_num, -- 成功发起otrike订单数，即拼车订单+包车订单
            --nvl(round(sum(carpool_success_num) / sum(carpool_num),2),0) as success_carpool_rate_d,  --拼成率：拼成 / 拼车
            nvl(sum(carpool_success_num),0) as carpool_success_order_num,-- 拼成的订单数量
            nvl(sum(carpool_accept_num),0) as carpool_accept_order_num,--拼单池内被应答的订单数量
            --nvl(round(sum(carpool_accept_num) / sum(carpool_num),2),0) as accept_carpool_rate_d,  --拼单应答率：拼车应答 / 拼车
            --nvl(round(sum(carpool_success_and_finish_num) / sum(carpool_num),2),0) as success_and_finish_carpool_rate_d, --拼车完单率：拼成完单 / 拼车
            nvl(sum(carpool_success_and_finish_num),0) as carpool_success_and_finish_order_num,--拼车成功且完成的order数量
            country_code,
            dt 
        from oride_dw.dm_oride_order_base_d 
        where dt = '{pt}'  and product_id = 3
        group by city_id,product_id,country_code,dt
    )ob
    inner join
    (
        select 
            city_id,city_name 
        from oride_dw.dim_oride_city
        where dt = '{pt}'
    )oc on ob.city_id = oc.city_id
    
    union all
    --全国日数据
    select
        0 as city_id,
        'All' as city_name,
        ob_all.carpool_order_num as carpool_order_num_d,
        ob_all.otrike_order_num as otrike_order_num_d ,
        ob_all.carpool_success_order_num as carpool_success_order_num_d ,
        ob_all.carpool_accept_order_num as carpool_accept_order_num_d,
        ob_all.carpool_success_and_finish_order_num as carpool_success_and_finish_order_num_d,
        ob_all.country_code as country_code,
        ob_all.dt as dt
    from 
    (
        select 
            --city_id,
            --product_id,
            nvl(sum(carpool_num),0) as carpool_order_num, --拼车订单数
            nvl(sum(carpool_num)+sum(chartered_bus_num),0) as otrike_order_num, -- 成功发起otrike订单数，即拼车订单+包车订单
            nvl(sum(carpool_success_num),0) as carpool_success_order_num,-- 拼成的订单数量
            nvl(sum(carpool_accept_num),0) as carpool_accept_order_num,--拼单池内被应答的订单数量
            nvl(sum(carpool_success_and_finish_num),0) as carpool_success_and_finish_order_num,--拼车成功且完成的order数量
            country_code,
            dt 
        from oride_dw.dm_oride_order_base_d 
        where dt = '{pt}'  and product_id = 3
        group by country_code,dt--city_id,product_id,country_code,dt
    )ob_all
)d

left join
--周数据
(
    --城市周
    select
        ob.city_id as city_id,
        oc.city_name as city_name,
        ob.carpool_order_num as carpool_order_num_w ,
        ob.otrike_order_num as otrike_order_num_w,
        ob.carpool_success_order_num as carpool_success_order_num_w,
        ob.carpool_accept_order_num as carpool_accept_order_num_w,
        ob.carpool_success_and_finish_order_num as carpool_success_and_finish_order_num_w,
        concat(year('{pt}')*100 + weekofyear('{pt}'),'w') as week_num
    from 
    (
        select 
            city_id,
            product_id,
            nvl(sum(carpool_num),0) as carpool_order_num, --拼车订单数
            nvl(sum(carpool_num)+sum(chartered_bus_num),0) as otrike_order_num, -- 成功发起otrike订单数，即拼车订单+包车订单
            nvl(sum(carpool_success_num),0) as carpool_success_order_num,-- 拼成的订单数量
            nvl(sum(carpool_accept_num),0) as carpool_accept_order_num,--拼单池内被应答的订单数量
            nvl(sum(carpool_success_and_finish_num),0) as carpool_success_and_finish_order_num--拼车成功且完成的order数量
        from oride_dw.dm_oride_order_base_d 
        --where dt = '{pt}'  and product_id = 3
        where year(dt) + weekofyear(dt) = year('{pt}')+weekofyear('{pt}')
            and product_id =3 
        group by city_id,product_id
    )ob
    inner join
    (
        select 
            city_id,city_name 
        from oride_dw.dim_oride_city
        where dt = '{pt}'--这个直接写死 '{pt}'
        
    )oc on ob.city_id = oc.city_id
    
    union all
    --全国周
    select
        0 as city_id,
        'All' as city_name,
        ob_all.carpool_order_num as carpool_order_num_w,
        ob_all.otrike_order_num as otrike_order_num_w,
        ob_all.carpool_success_order_num as carpool_success_order_num_w,
        ob_all.carpool_accept_order_num as carpool_accept_order_num_w,
        ob_all.carpool_success_and_finish_order_num as carpool_success_and_finish_order_num_w,
        concat(year('{pt}')*100 + weekofyear('{pt}'),'w') as week_num
    from 
    (
        select 
            --city_id,
            --product_id,
            nvl(sum(carpool_num),0) as carpool_order_num, --拼车订单数
            nvl(sum(carpool_num)+sum(chartered_bus_num),0) as otrike_order_num,-- 成功发起otrike订单数，即拼车订单+包车订单
            nvl(sum(carpool_success_num),0) as carpool_success_order_num,-- 拼成的订单数量
            nvl(sum(carpool_accept_num),0) as carpool_accept_order_num,--拼单池内被应答的订单数量
            nvl(sum(carpool_success_and_finish_num),0) as carpool_success_and_finish_order_num--拼车成功且完成的order数量
        from oride_dw.dm_oride_order_base_d 
        where year(dt) + weekofyear(dt) = year('{pt}')+weekofyear('{pt}')
            and product_id = 3
    )ob_all
)w on d.city_id = w.city_id
left join
--月数据
(
--城市月
    select
        ob.city_id as city_id,
        oc.city_name as city_name,
        ob.carpool_order_num as carpool_order_num_m,
        ob.otrike_order_num as otrike_order_num_m,
        ob.carpool_success_order_num as carpool_success_order_num_m,
        ob.carpool_accept_order_num as carpool_accept_order_num_m,
        ob.carpool_success_and_finish_order_num as carpool_success_and_finish_order_num_m,
        --ob.country_code,
        concat(year('{pt}')*100 + month('{pt}'),'m') as month_num
    from 
    (
        select 
            city_id,
            product_id,
            nvl(sum(carpool_num),0) as carpool_order_num, --拼车订单数
            nvl(sum(carpool_num)+sum(chartered_bus_num),0) as otrike_order_num,-- 成功发起otrike订单数，即拼车订单+包车订单
            nvl(sum(carpool_success_num),0) as carpool_success_order_num,-- 拼成的订单数量
            nvl(sum(carpool_accept_num),0) as carpool_accept_order_num,--拼单池内被应答的订单数量
            nvl(sum(carpool_success_and_finish_num),0) as carpool_success_and_finish_order_num--拼车成功且完成的order数量
        from oride_dw.dm_oride_order_base_d 
        --where dt = '{pt}'  and product_id = 3
        where year(dt) + month(dt) = year('{pt}')+month('{pt}')
            and product_id =3 
        group by city_id,product_id
    )ob
    inner join
    (
        select 
            city_id,city_name 
        from oride_dw.dim_oride_city
        where dt = '{pt}'--这个直接写死 '{pt}'
        
    )oc on ob.city_id = oc.city_id
    
    union all
    
    select
        0 as city_id,
        'All' as city_name,
        ob_all.carpool_order_num as carpool_order_num_m,
        ob_all.otrike_order_num as otrike_order_num_m,
        ob_all.carpool_success_order_num as carpool_success_order_num_m,
        ob_all.carpool_accept_order_num as carpool_accept_order_nume_m,
        ob_all.carpool_success_and_finish_order_num as carpool_success_and_finish_order_num_m,
        concat(year('{pt}')*100 + month('{pt}'),'m') as month_num
    from 
    (
        select 
            nvl(sum(carpool_num),0) as carpool_order_num, --拼车订单数
            nvl(sum(carpool_num)+sum(chartered_bus_num),0) as otrike_order_num, -- 成功发起otrike订单数，即拼车订单+包车订单
            nvl(sum(carpool_success_num),0) as carpool_success_order_num,-- 拼成的订单数量
            nvl(sum(carpool_accept_num),0) as carpool_accept_order_num,--拼单池内被应答的订单数量
            nvl(sum(carpool_success_and_finish_num),0) as carpool_success_and_finish_order_num--拼车成功且完成的order数量
        from oride_dw.dm_oride_order_base_d 
        where year(dt) + month(dt) = year('{pt}')+month('{pt}')
            and product_id = 3
    )ob_all
) m on d.city_id = m.city_id;
    
'''.format(
        pt=ds,
        table=table_name,
        db=db_name
        )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_oride_otrike_order_carpool_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    #check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


app_oride_otrike_order_carpool_d_task = PythonOperator(
    task_id='app_oride_otrike_order_carpool_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dependence_dm_oride_order_base_d_prev_day_task >> \
dim_oride_city_task >> \
app_oride_otrike_order_carpool_d_task
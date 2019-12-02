# -*- coding: utf-8 -*-
"""全局运营日报 to mysql"""

import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
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
    'start_date': datetime(2019, 11, 4),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_order_global_operate_to_mysql_d',
                  schedule_interval="30 3 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

dependence_dwm_oride_order_base_di_task = UFileSensor(
    task_id='dwm_oride_order_base_di_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_order_base_di",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dependence_dim_oride_city_task = HivePartitionSensor(
    task_id="dim_oride_city_task",
    table="dim_oride_city",
    partition="dt='{{ds}}'",
    schema="oride_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dependence_dim_oride_passenger_base_task = HivePartitionSensor(
    task_id="dim_oride_passenger_base_task",
    table="dim_oride_passenger_base",
    partition="dt='{{ds}}'",
    schema="oride_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dependence_dim_oride_driver_base_task = HivePartitionSensor(
    task_id="dim_oride_driver_base_task",
    table="dim_oride_driver_base",
    partition="dt='{{ds}}'",
    schema="oride_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


dependence_dwd_oride_order_finance_df_task = UFileSensor(
    task_id='dwd_oride_order_finance_df_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_finance_df",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dependence_dwd_oride_driver_records_day_df_task = UFileSensor(
    task_id='dwd_oride_driver_records_day_df_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_driver_records_day_df",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dependence_dwd_oride_driver_recharge_records_df_task = UFileSensor(
    task_id='dwd_oride_driver_recharge_records_df_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_driver_recharge_records_df",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)



dependence_dm_oride_driver_base_task = UFileSensor(
    task_id='dm_oride_driver_base_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dm_oride_driver_base",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dependence_dm_oride_passenger_base_cube_d_task = UFileSensor(
    task_id='dm_oride_passenger_base_cube_d_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dm_oride_passenger_base_cube_d",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)


dependence_dwd_oride_order_base_include_test_df_task = UFileSensor(
    task_id='dwd_oride_order_base_include_test_df_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_df",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_order_global_operate_to_mysql_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "2400"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def app_oride_order_global_operate_to_mysql_d_sql_task(ds):
    HQL = '''
      SET hive.exec.parallel=TRUE;
      set hive.exec.dynamic.partition.mode=nonstrict;    
         --将数据加载到内存，临时表
         
         
         
     with 
        dwd_order_df as
        ( 
            select
                city_id,
                price,
                driver_id,
                pay_amount,
                create_date,
                is_finish,
                create_time,
                dt 
            from oride_dw.dwd_oride_order_base_include_test_df
            where dt = '{pt}' and city_id != 999001 and is_finish=1
        )
    
    insert overwrite table oride_dw.{table} partition(country_code,dt)
    
    select 
        nvl(od.city_id,-10000) as city_id,
        nvl(city.city_name,'-10000') as city_name,
        nvl(od.order_cnt,0) as order_cnt, --下单量
        nvl(od.finish_order_cnt,0) as finish_order_cnt ,--完单量
        nvl(od.valid_ord_cnt,0) as valid_ord_cnt,--有效下单量
        nvl(od.wet_order_cnt,0) as wet_order_cnt,--湿单量
        nvl(od.order_distance,0) as order_distance,--送驾距离
        nvl(od.gmv,0) as gmv,--GMV
        nvl(sub.b_subsidy_d,0) as b_subsidy_d,--B端补贴/天
        nvl(sub.c_subsidy_d,0) as c_subsidy_d,--C端补贴/天
        nvl(sub.sum_subsidy_d,0) as sum_subsidy_d,--总补贴/天
        nvl(sub.sum_subsidy_m,0) as sum_subsidy_m,--累计总补贴/月
        nvl(od.finish_order_driver_num,0) as finish_order_driver_num,--完单司机数
        nvl(new_driver.new_finished_drivers,0) as new_finished_drivers,--新增完单司机数
        nvl(audit.td_audit_finish_driver_num,0) as td_audit_finish_driver_num,----审核通过司机数
        nvl( round(od.finish_order_cnt / od.finish_order_driver_num, 2),0) as avg_driver_finished_ord_cnt, -- 司机人均完单数
        nvl(round( od.order_distance /  od.finish_order_cnt,2),0) as avg_order_distance,--单均里程
        nvl(amount.avg_finish_driver_amount,0) avg_finish_driver_amount, --司机人均收入
        nvl(users.finished_users,0) as finished_users,----完单乘客数
        nvl(users.first_finished_users,0)  as new_finished_users,----新增完单乘客数
        nvl(round(od.wet_order_cnt / od.order_cnt,8),0) as wet_order_rate,--湿单占比 
        'nal' as country_code,
        '{pt}' as  dt
    from 
    (
        select 
            city_id,
            count(1) as order_cnt, --下单量
            sum(is_finish) as finish_order_cnt, --完单量
            sum(is_valid) as valid_ord_cnt,--有效订单量
            sum(if(is_finish =1,price,0)) as gmv, --gmv
            count(distinct if(is_finish =1 ,driver_id, null)) as finish_order_driver_num,--完单司机数
            count(if(is_wet_order =1,1,null)) as wet_order_cnt, --湿单量
            sum(if(is_finish = 1, order_onride_distance , 0)) as order_distance --送驾距离
        from  oride_dw.dwm_oride_order_base_di 
            where dt = '{pt}' 
        group by city_id with cube
    )od
    left join
    (   
        select
            city_id,
            city_name,
            dt
        from oride_dw.dim_oride_city
        where dt = '{pt}'
    )city on nvl(od.city_id,-10000) = city.city_id
    left join
    (
        select
            b.city_id,
        
            b.b_subsidy_d,
            --B端补贴/天
            --b_subsidy_m,
            --B端补贴/月
            c.c_subsidy_d,
            --C端补贴/天
            --c_subsidy_m,
            --C端补贴/月
            sum(b.b_subsidy_d + c_subsidy_d ) as sum_subsidy_d, 
            --总补贴/天
            sum(b_subsidy_m + c_subsidy_m ) as sum_subsidy_m,
            --累计总补贴/月
            b.dt
        from
        ( --B端补贴  t1.recharge_amount+t1.reward_amount
                        select 
                city_id,
                
                sum(if(create_date ='2019-11-01',reward_amount,0))+sum(if(create_date ='2019-11-01',recharge_amount,0)) as b_subsidy_d,--B端补贴、天
                    
                sum(reward_amount) + sum(recharge_amount) as b_subsidy_m,--B端补贴 月
                dt
            from oride_dw.dwd_oride_order_finance_df
            where dt ='2019-11-01' and month(create_date) = month('2019-11-01')
            group by city_id,dt
        )b
        left join
        (  --C端补贴  price - pay_amount is_finish=1
           select 
                city_id,
                sum(if(create_date ='2019-11-01',price,0))-sum(if(create_date ='2019-11-01',pay_amount,0)) as c_subsidy_d,--C端补贴、天     
                sum(price) - (pay_amount) as c_subsidy_m,
                dt
            from  dwd_order_df
            where dt = '2019-11-01' and city_id != 999001 and is_finish=1
            and   month(create_date) = month('2019-11-01')
            group by city_id,dt
        )c on  b.city_id =  c.city_id  and  c.dt =  b.dt
        group by b.city_id,b.dt,b.b_subsidy_d,c.c_subsidy_d
    )sub on  nvl(od.city_id,-10000) = sub.city_id 
    left join 
    (--审核司机数
        select 
            city_id,
            sum(td_audit_finish_driver_num) as td_audit_finish_driver_num --审核司机数
        from oride_dw.dm_oride_driver_base
        where dt ='{pt}' 
        group by city_id with cube
    )audit on nvl(od.city_id, -10000) = nvl( audit.city_id, -10000)
    left join
    (--当日完单用户数，当新增日完单用户数
        select 
            city_id,
            finished_users,--当日完单用户数
            first_finished_users,--当日新增完单用户
            dt
        from  oride_dw.dm_oride_passenger_base_cube_d
        where dt ='{pt}' 
        and product_id = -10000 and driver_serv_type = -10000
    )users on nvl(od.city_id,-10000) = nvl(users.city_id ,-10000)
    
    left join
    (--新增完单司机数
        select
            new.city_id,
            count(if(old.driver_id is null,new.driver_id,null)) as new_finished_drivers
            
        from 
        ( --今日完单司机
            select 
                city_id,
                driver_id
            from dwd_order_df
            where create_date = '{pt}'
            group by   city_id,driver_id,dt 
        )new
        left join
        (--以前的完单司机数
            select 
                driver_id
            
            from oride_dw.dwd_oride_order_base_include_test_df
            where dt in ( '{pt}','his') and create_date < '{pt}' and city_id != 999001 and is_finish =1 
            group by   driver_id
        )old on new.driver_id = old.driver_id
        group by new.city_id with cube
    )new_driver on nvl(od.city_id,-10000) = nvl(new_driver.city_id,-10000)
    
    left join
    (--司机人均实收
        select 
            tmp.city_id,
            round(tmp.finish_driver_amount / tmp.finish_driver_cnt , 2) as avg_finish_driver_amount,
            tmp.dt
        from
        (
            select 
                t.city_id,
                t.product_id,
                t.finish_driver_amount,
                t.finish_driver_cnt,
                row_number() over(partition by t.city_id order by finish_driver_cnt desc) rn,
                t.dt
            from 
            (
                --司机的收入
                select 
                    od.city_id,
                    driver.product_id,--完单司机业务线
                    sum(nvl(drd.amount_all,0) + nvl(drd.amount_agenter,0) - nvl(drr.amount,0))  as finish_driver_amount, --完单司机收入
                    count(od.driver_id) finish_driver_cnt, --完单司机数
                    od.dt
                from 
                (  --首先 找 完单司机
                    select
                     driver_id,
                     city_id,
                     dt
                    from  oride_dw.dwm_oride_order_base_di 
                   where dt = '{pt}' and is_finish = 1
                    group by driver_id,city_id,dt
                )od
                left join
                ( --司机业务线
                    select 
                        driver_id,
                        city_id,
                        product_id,
                        dt
                    from oride_dw.dim_oride_driver_base 
                    where dt = '{pt}'
                )driver on od.city_id = driver.city_id and driver.driver_id = od.driver_id
                left join 
                (--司机总收入 amount_all
                    select
                        driver_id,
                        nvl(amount_all,0) as amount_all,
                        nvl(amount_agenter,0) as amount_agenter,
                        from_unixtime(day,'yyyy-MM-dd') as day_date
                    from oride_dw.dwd_oride_driver_records_day_df
                    where dt = '{pt}' and  from_unixtime(day,'yyyy-MM-dd') = '{pt}'
                )drd on od.driver_id = drd.driver_id
                left join 
                (--司机amount
                
                    SELECT  
                        driver_id,
                        abs(nvl(amount,0)) as amount, 
                        from_unixtime(created_at, "yyyy-MM-dd")  as created_at 
                    FROM oride_dw.dwd_oride_driver_recharge_records_df  
                    WHERE dt = '{pt}' and from_unixtime(created_at,'yyyy-MM-dd') = '{pt}' 
                    and amount_reason=6 
                )drr on od.driver_id = drr.driver_id
                group by od.city_id,driver.product_id,od.dt
            )t
        )tmp 
        where tmp.rn = 1
    )amount on od.city_id = amount.city_id ;'''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_oride_order_global_operate_to_mysql_d_sql_task(ds)

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


app_oride_order_global_operate_to_mysql_d_task = PythonOperator(
    task_id='app_oride_order_global_operate_to_mysql_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dependence_dwm_oride_order_base_di_task >> dependence_dim_oride_city_task >> dependence_dim_oride_passenger_base_task >> \
dependence_dim_oride_driver_base_task >> dependence_dwd_oride_order_finance_df_task >> dependence_dwd_oride_driver_records_day_df_task >> \
dependence_dwd_oride_driver_recharge_records_df_task >> dependence_dm_oride_driver_base_task>>dependence_dm_oride_passenger_base_cube_d_task>>\
dependence_dwd_oride_order_base_include_test_df_task>>app_oride_order_global_operate_to_mysql_d_task
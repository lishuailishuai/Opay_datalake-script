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
from airflow.sensors.s3_key_sensor import S3KeySensor
import json
import logging
from airflow.models import Variable
import requests
import os
from airflow.hooks.hive_hooks import HiveCliHook
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from plugins.CountriesAppFrame import CountriesAppFrame
from airflow.operators.python_operator import PythonOperator
from airflow.sensors import OssSensor

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2019, 10, 14),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_cohort_w',
                  schedule_interval="30 03 * * 1",
                  default_args=args,
                  )

##----------------------------------------- 依赖 ---------------------------------------##
dwm_oride_order_base_di_task = OssSensor(
        task_id='dwm_oride_order_base_di_task',
        bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_order_base_di",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )

dwm_oride_passenger_base_df_task = OssSensor(
        task_id='dwm_oride_passenger_base_df_task',
        bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_passenger_base_df",
            pt='{{macros.ds_add(ds, +6)}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )

dwm_oride_driver_base_df_task = OssSensor(
        task_id='dwm_oride_driver_base_df_task',
        bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_driver_base_df",
            pt='{{macros.ds_add(ds, +6)}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )

dwm_oride_passenger_act_w_task = OssSensor(
        task_id='dwm_oride_passenger_act_w_task',
        bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_passenger_act_w",
            pt='{{macros.ds_add(ds, +6)}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )

dwm_oride_driver_act_w_task = OssSensor(
        task_id='dwm_oride_driver_act_w_task',
        bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_driver_act_w",
            pt='{{macros.ds_add(ds, +6)}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )


##----------------------------------------- 变量 ---------------------------------------##
def get_table_info(i):
    table_names = ['app_oride_new_user_cohort_w',
                       'app_oride_new_driver_cohort_w',
                       'app_oride_act_user_cohort_w',
                       'app_oride_act_driver_cohort_w']
    hdfs_paths = "oss://opay-datalake/oride/oride_dw/"
    return table_names[i], hdfs_paths + table_names[i]

db_name = "oride_dw"

def get_max_week(ds):
    sql = '''
        select 
            max(week) as max_week
        from oride_dw.dwm_oride_passenger_act_w
        where datediff('{pt}',dt)<=90 and datediff('{pt}',dt)>=0
    '''.format(
        pt=airflow.macros.ds_add(ds, +6)
    )

    cursor = get_hive_cursor()
    logging.info('Executing: %s', sql)
    cursor.execute(sql)
    week_list = cursor.fetchall()
    cursor.close
    if len(week_list) > 0:
        for week in week_list:
            max_week = week[0]
    return max_week

##----------------------------------------- 脚本 ---------------------------------------##


def app_oride_new_user_cohort_w_sql_task(ds):

    HQL='''set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    -- set hive.merge.mapredfiles=true;
    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
        --乘客新客留存数据统计【上线后的统计，上线后用每周的数据关联历史所有周的乘客新客数据】
        
        select nvl(week_create_date,-10000) as week_create_date,
            nvl(m.weeks,-10000) as weeks,
            nvl(m.city_id,-10000) as city_id,
            nvl(product_id,-10000) as product_id,
            new_user_liucun_cnt, --第一周对应的就是新客
            new_user_liucun_ord_cnt, --新客留存完单量
            new_user_liucun_gmv, --新客留存完单gmv
            new_user_liucun_dis, --新客留存完单里程
            'nal' as country_code,
            '{pt}' as dt
        from (
            select nvl(a.week_create_date,-10000) as week_create_date,
                nvl(
                    if(a.week_create_date<new_user.week_create_date,
                        a.week_create_date-new_user.week_create_date+{max_week},
                        a.week_create_date-new_user.week_create_date)
                ,-10000) as weeks,
                nvl(a.city_id,-10000) as city_id,
                nvl(a.product_id,-10000) as product_id,
                count(distinct (nvl(new_user.passenger_id,null))) as new_user_liucun_cnt,  --第一周对应的就是新客
                count(if(new_user.passenger_id is not null,a.order_id,null)) as new_user_liucun_ord_cnt, --新客留存完单量
                sum(if(new_user.passenger_id is not null,a.price,0)) as new_user_liucun_gmv, --新客留存完单gmv
                sum(if(new_user.passenger_id is not null,a.distance,0)) as new_user_liucun_dis --新客留存完单里程     
                
            from (
                select passenger_id,city_id,
                    driver_serv_type as product_id,
                    order_id,price,
                    order_onride_distance as distance,
                    weekofyear(dt) as week_create_date
                from oride_dw.dwm_oride_order_base_di
                where dt between date_sub('{pt}',6) and '{pt}'
                and is_finish=1 and city_id<>999001 and driver_id<>1
            )as a
            left join
            (
                select first_finish_city_id as city_id,
                    first_finish_product_id as product_id,
                    passenger_id,
                    weekofyear(first_finish_create_date)as week_create_date
                from oride_dw.dwm_oride_passenger_base_df
                where dt='{pt}' 
                and first_finish_ord_id is not null 
                and datediff('{pt}',first_finish_create_date)<=90
                and datediff('{pt}',first_finish_create_date)>=0
                group by weekofyear(first_finish_create_date),first_finish_city_id,first_finish_product_id,passenger_id
            )as new_user
            on a.city_id=new_user.city_id
            and a.product_id=new_user.product_id
            and a.passenger_id=new_user.passenger_id
        
            group by nvl(a.week_create_date,-10000),
            nvl(
                if(a.week_create_date<new_user.week_create_date,
                    a.week_create_date-new_user.week_create_date+{max_week},
                    a.week_create_date-new_user.week_create_date)
            ,-10000),
            nvl(a.city_id,-10000),
            nvl(a.product_id,-10000)
            with cube
        ) m
        where !(nvl(m.week_create_date,-10000)=-10000 or nvl(m.weeks,-10000)=-10000);
                 '''.format(
        pt=airflow.macros.ds_add(ds, +6),
        table=get_table_info(0)[0],
        max_week=get_max_week(ds)
    )
    return HQL


def app_oride_new_driver_cohort_w_sql_task(ds):

    HQL='''set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    -- set hive.merge.mapredfiles=true;
     INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
        --司机新客留存数据统计【上线后的统计，上线后用每周的数据关联历史所有周的司机新客数据,但是由于每天有些订单并不是终态数据，因此每次都需要重新判定新客和活跃】
        
        select nvl(m.week_create_date,-10000) as week_create_date,
            nvl(m.weeks,-10000) as weeks,
            nvl(m.city_id,-10000) as city_id,
            nvl(m.product_id,-10000) as product_id,
            new_driver_liucun_cnt,  --第一周对应的就是新客
            new_driver_liucun_ord_cnt, --新客留存完单量
            new_driver_liucun_gmv, --新客留存完单gmv
            new_driver_liucun_dis, --新客留存完单里程
            'nal' as country_code,
            '{pt}' as dt
        from (
            select nvl(a.week_create_date,-10000) as week_create_date,
            nvl(
                if(a.week_create_date<new_driver.week_create_date,
                    a.week_create_date-new_driver.week_create_date+{max_week},
                    a.week_create_date-new_driver.week_create_date)
            ,-10000) as weeks,
            nvl(a.city_id,-10000) as city_id,
            nvl(a.product_id,-10000) as product_id,
            count(distinct (nvl(new_driver.driver_id,null))) as new_driver_liucun_cnt,  --第一周对应的就是新客
            count(if(new_driver.driver_id is not null,a.order_id,null)) as new_driver_liucun_ord_cnt, --新客留存完单量
            sum(if(new_driver.driver_id is not null,a.price,0)) as new_driver_liucun_gmv, --新客留存完单gmv
            sum(if(new_driver.driver_id is not null,a.distance,0)) as new_driver_liucun_dis --新客留存完单里程
        
            from (
                select driver_id,city_id,
                    driver_serv_type as product_id,
                    price,order_id,
                    order_onride_distance as distance,
                    weekofyear(dt) as week_create_date
                from oride_dw.dwm_oride_order_base_di
                where dt between date_sub('{pt}',6) and '{pt}' 
                and is_finish=1 and city_id<>999001 and driver_id<>1
            )as a
            left join
            (
                select city_id,product_id,driver_id,
                    weekofyear(from_unixtime(first_finish_order_create_time,'yyyy-MM-dd')) as week_create_date
                from oride_dw.dwm_oride_driver_base_df
                where dt='{pt}' 
                and first_finish_order_id is not null
                and datediff('{pt}',from_unixtime(first_finish_order_create_time,'yyyy-MM-dd'))<=90
                and datediff('{pt}',from_unixtime(first_finish_order_create_time,'yyyy-MM-dd'))>=0
                group by weekofyear(from_unixtime(first_finish_order_create_time,'yyyy-MM-dd')),
                    city_id,product_id,driver_id
            )as new_driver
            on a.city_id=new_driver.city_id
            and a.product_id=new_driver.product_id
            and a.driver_id=new_driver.driver_id
            
            group by nvl(a.week_create_date,-10000),
            nvl(
                if(a.week_create_date<new_driver.week_create_date,
                    a.week_create_date-new_driver.week_create_date+{max_week},
                    a.week_create_date-new_driver.week_create_date)
            ,-10000),
            nvl(a.city_id,-10000),
            nvl(a.product_id,-10000)
            with cube
        ) m
        where !(nvl(m.week_create_date,-10000)=-10000 or nvl(m.weeks,-10000)=-10000); 
                 '''.format(
        pt=airflow.macros.ds_add(ds, +6),
        table=get_table_info(1)[0],
        max_week=get_max_week(ds)
    )
    return HQL


def app_oride_act_user_cohort_w_sql_task(ds):

    HQL='''set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    -- set hive.merge.mapredfiles=true;
     INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
        --活跃乘客留存数据统计【上线后的统计，上线后用每周的数据关联历史所有周的活跃乘客数据】
        select nvl(m.week_create_date,-10000) as week_create_date,
            nvl(m.weeks,-10000) as weeks,
            nvl(m.city_id,-10000) as city_id,
            nvl(m.product_id,-10000) as product_id,
            act_user_liucun_cnt,  --第一周对应的就是活跃乘客数
            act_user_liucun_ord_cnt, --活跃乘客留存完单量
            act_user_liucun_gmv, --活跃乘客留存完单gmv
            act_user_liucun_dis, --活跃乘客留存完单里程
            'nal' as country_code,
            '{pt}' as dt
        from(
            select nvl(a.week_create_date,-10000) as week_create_date,
            nvl(
                if(a.week_create_date<act_user.week_create_date,
                    a.week_create_date-act_user.week_create_date+{max_week},
                    a.week_create_date-act_user.week_create_date)
            ,-10000) as weeks,
            nvl(a.city_id,-10000) as city_id,
            nvl(a.product_id,-10000) as product_id,
            count(distinct act_user.passenger_id) as act_user_liucun_cnt,  --第一周对应的就是活跃乘客数
            count(if(act_user.passenger_id is not null,a.order_id,null)) as act_user_liucun_ord_cnt, --活跃乘客留存完单量
            sum(if(act_user.passenger_id is not null,a.price,0)) as act_user_liucun_gmv, --活跃乘客留存完单gmv
            sum(if(act_user.passenger_id is not null,a.distance,0)) as act_user_liucun_dis --活跃乘客留存完单里程
        
            from (
                select passenger_id,city_id,
                    driver_serv_type as product_id,
                    order_id,price,
                    order_onride_distance as distance,
                    weekofyear(dt) as week_create_date
                from oride_dw.dwm_oride_order_base_di
                where dt between date_sub('{pt}',6) and '{pt}'
                and is_finish=1 and city_id<>999001 and driver_id<>1
            )as a
            left join
            (
                select week as week_create_date,
                    city_id,
                    product_id,
                    passenger_id 
                from oride_dw.dwm_oride_passenger_act_w
                where datediff('{pt}',dt)<=90 and datediff('{pt}',dt)>=0
                group by week,city_id,product_id,passenger_id
            )as act_user
            on a.city_id=act_user.city_id
            and a.product_id=act_user.product_id
            and a.passenger_id=act_user.passenger_id
            --where a.week_create_date>=act_user.week_create_date
            group by nvl(a.week_create_date,-10000),
            nvl(
                if(a.week_create_date<act_user.week_create_date,
                    a.week_create_date-act_user.week_create_date+{max_week},
                    a.week_create_date-act_user.week_create_date)
            ,-10000),
            nvl(a.city_id,-10000),
            nvl(a.product_id,-10000)
            with cube
        ) m
        where !(nvl(m.week_create_date,-10000)=-10000 or nvl(m.weeks,-10000)=-10000); 
        
                 '''.format(
        pt=airflow.macros.ds_add(ds, +6),
        table=get_table_info(2)[0],
        max_week=get_max_week(ds)
    )
    return HQL


def app_oride_act_driver_cohort_w_sql_task(ds):

    HQL='''set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    -- set hive.merge.mapredfiles=true;
    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
        --活跃司机留存数据统计【上线后的统计，上线后用每周的数据关联历史所有周的活跃司机数据】
        
        select nvl(m.week_create_date,-10000) as week_create_date,
            nvl(m.weeks,-10000) as weeks,
            nvl(m.city_id,-10000) as city_id,
            nvl(m.product_id,-10000) as product_id,
            act_driver_liucun_cnt,  --第一周对应的就是活跃司机数
            act_driver_liucun_ord_cnt, --活跃司机留存完单量
            act_driver_liucun_gmv, --活跃司机留存完单gmv
            act_driver_liucun_dis, --活跃司机留存完单里程
            'nal' as country_code,
            '{pt}' as dt
        
        from(
            select nvl(a.week_create_date,-10000) as week_create_date,
            nvl(
                if(a.week_create_date<act_driver.week_create_date,
                    a.week_create_date-act_driver.week_create_date+{max_week},
                    a.week_create_date-act_driver.week_create_date)
            ,-10000) as weeks,
            nvl(a.city_id,-10000) as city_id,
            nvl(a.product_id,-10000) as product_id,
            count(distinct act_driver.driver_id) as act_driver_liucun_cnt,  --第一周对应的就是活跃司机数
            count(if(act_driver.driver_id is not null,a.order_id,null)) as act_driver_liucun_ord_cnt, --活跃司机留存完单量
            sum(if(act_driver.driver_id is not null,a.price,0)) as act_driver_liucun_gmv, --活跃司机留存完单gmv
            sum(if(act_driver.driver_id is not null,a.distance,0)) as act_driver_liucun_dis --活跃司机留存完单里程
        
            from  (
                select driver_id,city_id,
                    driver_serv_type as product_id,
                    price,order_id,
                    order_onride_distance as distance,
                    weekofyear(dt) as week_create_date
                from oride_dw.dwm_oride_order_base_di
                where dt between date_sub('{pt}',6) and '{pt}' 
                and is_finish=1 and city_id<>999001 and driver_id<>1
            )as a
            left join
            (
                select week as week_create_date,
                    city_id,
                    product_id,
                    driver_id 
                from oride_dw.dwm_oride_driver_act_w 
                where datediff('{pt}',dt)<=90 and datediff('{pt}',dt)>=0
                group by week,city_id,product_id,driver_id
            )as act_driver
            on a.city_id=act_driver.city_id
            and a.product_id=act_driver.product_id
            and a.driver_id=act_driver.driver_id
            --where a.week_create_date>=act_driver.week_create_date
            group by nvl(a.week_create_date,-10000),
            nvl(
                if(a.week_create_date<act_driver.week_create_date,
                    a.week_create_date-act_driver.week_create_date+{max_week},
                    a.week_create_date-act_driver.week_create_date)
            ,-10000),
            nvl(a.city_id,-10000),
            nvl(a.product_id,-10000)
            with cube
        ) m
        where !(nvl(m.week_create_date,-10000)=-10000 or nvl(m.weeks,-10000)=-10000); 
                 '''.format(
        pt=airflow.macros.ds_add(ds, +6),
        table=get_table_info(3)[0],
        max_week=get_max_week(ds)
    )
    return HQL

#主流程
def execution_new_user_task(ds,dag,**kwargs):

    v_date=kwargs.get('v_execution_date')
    v_day=kwargs.get('v_execution_day')
    v_hour=kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    hdfs_path = get_table_info(0)[1]
    table_name=get_table_info(0)[0]

    args = [
        {
            "dag": dag,
            "is_countries_online": "false",
            "db_name": db_name,
            "table_name": table_name,
            "data_oss_path": hdfs_path,
            "is_country_partition": "true",
            "is_result_force_exist": "false",
            "execute_time": v_date,
            "is_hour_task": "false",
            "frame_type": "local",
            "is_offset": "true",
            "execute_time_offset": -1,
            "business_key": "oride"
        }
    ]

    cf = CountriesAppFrame(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + app_oride_new_user_cohort_w_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()

def execution_new_driver_task(ds,dag,**kwargs):

    v_date=kwargs.get('v_execution_date')
    v_day=kwargs.get('v_execution_day')
    v_hour=kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    hdfs_path = get_table_info(1)[1]
    table_name=get_table_info(1)[0]

    args = [
        {
            "dag": dag,
            "is_countries_online": "false",
            "db_name": db_name,
            "table_name": table_name,
            "data_oss_path": hdfs_path,
            "is_country_partition": "true",
            "is_result_force_exist": "false",
            "execute_time": v_date,
            "is_hour_task": "false",
            "frame_type": "local",
            "is_offset": "true",
            "execute_time_offset": -1,
            "business_key": "oride"
        }
    ]

    cf = CountriesAppFrame(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + app_oride_new_driver_cohort_w_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()

def execution_act_user_task(ds,dag,**kwargs):

    v_date=kwargs.get('v_execution_date')
    v_day=kwargs.get('v_execution_day')
    v_hour=kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    hdfs_path = get_table_info(2)[1]
    table_name=get_table_info(2)[0]

    args = [
        {
            "dag": dag,
            "is_countries_online": "false",
            "db_name": db_name,
            "table_name": table_name,
            "data_oss_path": hdfs_path,
            "is_country_partition": "true",
            "is_result_force_exist": "false",
            "execute_time": v_date,
            "is_hour_task": "false",
            "frame_type": "local",
            "is_offset": "true",
            "execute_time_offset": -1,
            "business_key": "oride"
        }
    ]

    cf = CountriesAppFrame(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + app_oride_act_user_cohort_w_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()

def execution_act_driver_task(ds,dag,**kwargs):

    v_date=kwargs.get('v_execution_date')
    v_day=kwargs.get('v_execution_day')
    v_hour=kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    hdfs_path = get_table_info(3)[1]
    table_name=get_table_info(3)[0]

    args = [
        {
            "dag": dag,
            "is_countries_online": "false",
            "db_name": db_name,
            "table_name": table_name,
            "data_oss_path": hdfs_path,
            "is_country_partition": "true",
            "is_result_force_exist": "false",
            "execute_time": v_date,
            "is_hour_task": "false",
            "frame_type": "local",
            "is_offset": "true",
            "execute_time_offset": -1,
            "business_key": "oride"
        }
    ]

    cf = CountriesAppFrame(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + app_oride_act_driver_cohort_w_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()

app_oride_new_user_cohort_w_task = PythonOperator(
    task_id='app_oride_new_user_cohort_w_task',
    python_callable=execution_new_user_task,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

app_oride_new_driver_cohort_w_task = PythonOperator(
    task_id='app_oride_new_driver_cohort_w_task',
    python_callable=execution_new_driver_task,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

app_oride_act_user_cohort_w_task = PythonOperator(
    task_id='app_oride_act_user_cohort_w_task',
    python_callable=execution_act_user_task,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

app_oride_act_driver_cohort_w_task = PythonOperator(
    task_id='app_oride_act_driver_cohort_w_task',
    python_callable=execution_act_driver_task,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)


dwm_oride_order_base_di_task>>app_oride_new_user_cohort_w_task
dwm_oride_passenger_base_df_task>>app_oride_new_user_cohort_w_task
dwm_oride_order_base_di_task>>app_oride_new_driver_cohort_w_task
dwm_oride_driver_base_df_task>>app_oride_new_driver_cohort_w_task
dwm_oride_order_base_di_task>>app_oride_act_user_cohort_w_task
dwm_oride_passenger_act_w_task>>app_oride_act_user_cohort_w_task
dwm_oride_order_base_di_task>>app_oride_act_driver_cohort_w_task
dwm_oride_driver_act_w_task>>app_oride_act_driver_cohort_w_task
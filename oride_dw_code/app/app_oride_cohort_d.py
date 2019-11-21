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
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'chenghui',
    'start_date': datetime(2019, 11, 11),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_cohort_d',
                  schedule_interval="00 03 * * *",
                  default_args=args)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##


# 依赖前一天分区
dependence_dwd_oride_order_base_include_test_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_base_include_test_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
def get_table_info(i):
    table_names = ['app_oride_new_user_cohort_d',
                   'app_oride_new_driver_cohort_d',
                   'app_oride_act_user_cohort_d',
                   'app_oride_act_driver_cohort_d']
    hdfs_paths = "ufile://opay-datalake/oride/oride_dw/"
    return table_names[i], hdfs_paths + table_names[i]

##----------------------------------------- 脚本 ---------------------------------------##
create_oride_cohort_mid_d_task = HiveOperator(

    task_id='create_oride_cohort_mid_d_task',
    hql='''drop table if exists oride_dw.oride_cohort_mid_d ;
     create table oride_dw.oride_cohort_mid_d as 
           select day(to_date(dt)) as day_now, --当前所在天
            t2.day_of_year as day_create_date, --下单时间所在天（一年中的第几天）
            
            city_id,
            driver_serv_type as product_id,
            order_id, 
            passenger_id,
            driver_id,
            user_first_time,--乘客第一次完单时间
            day(user_first_time) as day_user_first_time, --乘客首次完单时间所在天
            month(user_first_time) as month_user_first_time, --乘客首次完单时间所在月
            weekofyear(to_date(substr(user_first_time,1,10))) as week_user_first_time, --乘客首次完单时间所在周
            if(user_first_time=create_time,1,0) as is_new_user, --是否新乘客
            driver_first_time,--司机第一次完单时间
            day(driver_first_time) as day_driver_first_time, --司机首次完单时间所在天
            month(driver_first_time) as month_driver_first_time, --司机首次完单时间所在月
            weekofyear(to_date(substr(driver_first_time,1,10))) as week_driver_first_time, --司机首次完单时间所在周
            if(driver_first_time=create_time,1,0) as is_new_driver, --是否新司机
            price,
            distance,
            create_date,
            create_time
            from 
            (  
                select dt,
                city_id,
                product_id,
                driver_serv_type,
                order_id,
                passenger_id,
                driver_id,
                min(from_unixtime(create_time,'yyyy-MM-dd HH:mm:ss')) over (partition by passenger_id) as user_first_time,--乘客第一次完单时间
                min(from_unixtime(create_time,'yyyy-MM-dd HH:mm:ss')) over (partition by driver_id) as driver_first_time,--司机第一次完单时间
                price,
                distance,
                create_date,
                from_unixtime(create_time,'yyyy-MM-dd HH:mm:ss') as create_time
                --min(unix_timestamp(create_time)) over (partition by passenger_id) as user_first_time,--乘客第一次完单时间
                --min(unix_timestamp(create_time)) over (partition by driver_id) as driver_first_time,--司机第一次完单时间
                from oride_dw.dwd_oride_order_base_include_test_di
                where dt>='2019-07-08'  --从20190705号开始加的city_id字段，因此从28周开始统计留存数据，按日的从20190705号开始统计
                and status in(4,5) and city_id<>999001 and driver_id<>1 and product_id<>99
            ) t
            left join 
            (select dt dt_date,
                day_of_year, --一年中的第几天
                month, --一年中的第几月
                week_of_year --一年中的第几周
            from public_dw_dim.dim_date 
            ) t2
            on t.create_date=t2.dt_date;
            '''.format(
        pt='{{ds}}'
    ),
    dag=dag)

app_oride_new_user_cohort_d_task = HiveOperator(

    task_id='app_oride_new_user_cohort_d_task',
    hql='''set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    -- set hive.merge.mapredfiles=true;
    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
        --乘客新客留存数据统计【上线后的统计，上线后用每天的数据关联历史所有天的乘客新客数据】
        select nvl(day_create_date,-10000) as day_create_date,
        nvl(m.days,-10000) as days,
        nvl(m.city_id,-10000) as city_id,
        nvl(product_id,-10000) as product_id,
        new_user_liucun_cnt, --第一天对应的就是新客
        new_user_liucun_ord_cnt, --新客留存完单量
        new_user_liucun_gmv, --新客留存完单gmv
        new_user_liucun_dis, --新客留存完单里程
        'nal' as country_code,
        '{pt}' as dt
        from (
            select nvl(a.day_create_date,-10000) as day_create_date,
            nvl((a.day_create_date-new_user.day_create_date),-10000) as days,
            nvl(a.city_id,-10000) as city_id,
            nvl(a.product_id,-10000) as product_id,
            count(distinct (nvl(new_user.passenger_id,null))) as new_user_liucun_cnt,  --第一天对应的就是新客
            count(if(new_user.passenger_id is not null,a.order_id,null)) as new_user_liucun_ord_cnt, --新客留存完单量
            sum(if(new_user.passenger_id is not null,a.price,0)) as new_user_liucun_gmv, --新客留存完单gmv
            sum(if(new_user.passenger_id is not null,a.distance,0)) as new_user_liucun_dis --新客留存完单里程     

            from oride_dw.oride_cohort_mid_d a
            left join
            (select order_id,city_id,product_id,passenger_id,is_new_user,day_create_date
                from oride_dw.oride_cohort_mid_d
                where is_new_user=1
            ) new_user
            on a.city_id=new_user.city_id
            and a.product_id=new_user.product_id
            and a.passenger_id=new_user.passenger_id

            group by nvl(a.day_create_date,-10000),
            nvl((a.day_create_date-new_user.day_create_date),-10000),
            nvl(a.city_id,-10000),
            nvl(a.product_id,-10000)
            with cube
        ) m
        where !(nvl(m.day_create_date,-10000)=-10000 or nvl(m.days,-10000)=-10000); 
                 '''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=get_table_info(0)[0]
    ),
    dag=dag)

app_oride_new_driver_cohort_d_task = HiveOperator(

    task_id='app_oride_new_driver_cohort_d_task',
    hql='''set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    -- set hive.merge.mapredfiles=true;
    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
        --司机新客留存数据统计【上线后的统计，上线后用每天的数据关联历史所有天的司机新客数据,但是由于每天有些订单并不是终态数据，因此每次都需要重新判定新客和活跃】
        select nvl(m.day_create_date,-10000) as day_create_date,
        nvl(m.days,-10000) as days,
        nvl(m.city_id,-10000) as city_id,
        nvl(m.product_id,-10000) as product_id,
        new_driver_liucun_cnt,  --第一天对应的就是新客
        new_driver_liucun_ord_cnt, --新客留存完单量
        new_driver_liucun_gmv, --新客留存完单gmv
        new_driver_liucun_dis, --新客留存完单里程
        'nal' as country_code,
        '{pt}' as dt
        from (select nvl(a.day_create_date,-10000) as day_create_date,
        nvl((a.day_create_date-new_driver.day_create_date),-10000) as days,
        nvl(a.city_id,-10000) as city_id,
        nvl(a.product_id,-10000) as product_id,
        count(distinct (nvl(new_driver.driver_id,null))) as new_driver_liucun_cnt,  --第一天对应的就是新客
        count(if(new_driver.driver_id is not null,a.order_id,null)) as new_driver_liucun_ord_cnt, --新客留存完单量
        sum(if(new_driver.driver_id is not null,a.price,0)) as new_driver_liucun_gmv, --新客留存完单gmv
        sum(if(new_driver.driver_id is not null,a.distance,0)) as new_driver_liucun_dis --新客留存完单里程


        from oride_dw.oride_cohort_mid_d a
        left join
        (select order_id,city_id,product_id,driver_id,is_new_driver,day_create_date
        from oride_dw.oride_cohort_mid_d
        where is_new_driver=1) new_driver
        on a.city_id=new_driver.city_id
        and a.product_id=new_driver.product_id
        and a.driver_id=new_driver.driver_id

        group by nvl(a.day_create_date,-10000),
        nvl((a.day_create_date-new_driver.day_create_date),-10000),
        nvl(a.city_id,-10000),
        nvl(a.product_id,-10000)
        with cube) m
        where !(nvl(m.day_create_date,-10000)=-10000 or nvl(m.days,-10000)=-10000); 
        '''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=get_table_info(1)[0]
    ),
    dag=dag)

app_oride_act_user_cohort_d_task = HiveOperator(

    task_id='app_oride_act_user_cohort_d_task',
    hql='''set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    -- set hive.merge.mapredfiles=true;
    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
        --活跃乘客留存数据统计【上线后的统计，上线后用每天的数据关联历史所有天的活跃乘客数据】
        select nvl(m.day_create_date,-10000) as day_create_date,
        nvl(m.days,-10000) as days,
        nvl(m.city_id,-10000) as city_id,
        nvl(m.product_id,-10000) as product_id,
        act_user_liucun_cnt,  --第一天对应的就是活跃乘客数
        act_user_liucun_ord_cnt, --活跃乘客留存完单量
        act_user_liucun_gmv, --活跃乘客留存完单gmv
        act_user_liucun_dis, --活跃乘客留存完单里程
        'nal' as country_code,
        '{pt}' as dt

        from(select nvl(a.day_create_date,-10000) as day_create_date,
        nvl((a.day_create_date-act_user.day_create_date),-10000) as days,
        nvl(a.city_id,-10000) as city_id,
        nvl(a.product_id,-10000) as product_id,
        count(distinct act_user.passenger_id) as act_user_liucun_cnt,  --第一天对应的就是活跃乘客数
        count(if(act_user.passenger_id is not null,a.order_id,null)) as act_user_liucun_ord_cnt, --活跃乘客留存完单量
        sum(if(act_user.passenger_id is not null,a.price,0)) as act_user_liucun_gmv, --活跃乘客留存完单gmv
        sum(if(act_user.passenger_id is not null,a.distance,0)) as act_user_liucun_dis --活跃乘客留存完单里程

        from oride_dw.oride_cohort_mid_d a
        left join
        (select day_create_date,city_id,product_id,passenger_id 
        from oride_dw.oride_cohort_mid_d 
        group by day_create_date,city_id,product_id,passenger_id) act_user
        on a.city_id=act_user.city_id
        and a.product_id=act_user.product_id
        and a.passenger_id=act_user.passenger_id
        where a.day_create_date>=act_user.day_create_date
        group by nvl(a.day_create_date,-10000),
        nvl((a.day_create_date-act_user.day_create_date),-10000),
        nvl(a.city_id,-10000),
        nvl(a.product_id,-10000)
        with cube) m
        where !(nvl(m.day_create_date,-10000)=-10000 or nvl(m.days,-10000)=-10000); 
                 '''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=get_table_info(2)[0]
    ),
    dag=dag)

app_oride_act_driver_cohort_d_task = HiveOperator(

    task_id='app_oride_act_driver_cohort_d_task',
    hql='''set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    -- set hive.merge.mapredfiles=true;
    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
        --活跃司机留存数据统计【上线后的统计，上线后用每天的数据关联历史所有天的活跃司机数据】
        select nvl(m.day_create_date,-10000) as day_create_date,
        nvl(m.days,-10000) as days,
        nvl(m.city_id,-10000) as city_id,
        nvl(m.product_id,-10000) as product_id,
        act_driver_liucun_cnt,  --第一天对应的就是活跃司机数
        act_driver_liucun_ord_cnt, --活跃司机留存完单量
        act_driver_liucun_gmv, --活跃司机留存完单gmv
        act_driver_liucun_dis, --活跃司机留存完单里程
        'nal' as country_code,
        '{pt}' as dt

        from(select nvl(a.day_create_date,-10000) as day_create_date,
        nvl((a.day_create_date-act_driver.day_create_date),-10000) as days,
        nvl(a.city_id,-10000) as city_id,
        nvl(a.product_id,-10000) as product_id,
        count(distinct act_driver.driver_id) as act_driver_liucun_cnt,  --第一天对应的就是活跃司机数
        count(if(act_driver.driver_id is not null,a.order_id,null)) as act_driver_liucun_ord_cnt, --活跃司机留存完单量
        sum(if(act_driver.driver_id is not null,a.price,0)) as act_driver_liucun_gmv, --活跃司机留存完单gmv
        sum(if(act_driver.driver_id is not null,a.distance,0)) as act_driver_liucun_dis --活跃司机留存完单里程

        from oride_dw.oride_cohort_mid_d a
        left join
        (select day_create_date,city_id,product_id,driver_id 
        from oride_dw.oride_cohort_mid_d 
        group by day_create_date,city_id,product_id,driver_id) act_driver
        on a.city_id=act_driver.city_id
        and a.product_id=act_driver.product_id
        and a.driver_id=act_driver.driver_id
        where a.day_create_date>=act_driver.day_create_date
        group by nvl(a.day_create_date,-10000),
        nvl((a.day_create_date-act_driver.day_create_date),-10000),
        nvl(a.city_id,-10000),
        nvl(a.product_id,-10000)
        with cube) m
        where !(nvl(m.day_create_date,-10000)=-10000 or nvl(m.days,-10000)=-10000); 
                 '''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=get_table_info(3)[0]
    ),
    dag=dag)

# 生成_SUCCESS
oride_cohort_mid_d_success = BashOperator(

    task_id='oride_cohort_mid_d_success',

    bash_command="""
    line_num=`$HADOOP_HOME/bin/hadoop fs -du -s hdfs://warehourse/user/hive/warehouse/oride_dw.db/oride_cohort_mid_d | tail -1 | awk '{{print $1}}'`

    if [ $line_num -eq 0 ]
    then
        echo "table oride_dw.oride_cohort_mid_d is empty"
        exit 1
    else
        echo "DATA EXPORT Successed ......"
    fi
    """.format(
        pt='{{ds}}'),
    dag=dag)

new_user_cohort_d_touchz_success = BashOperator(

    task_id='new_user_cohort_d_touchz_success',

    bash_command="""
    line_num=`$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk '{{print $1}}'`

    if [ $line_num -eq 0 ]
    then
        echo "FATAL {hdfs_data_dir} is empty"
        exit 1
    else
        echo "DATA EXPORT Successed ......"
        $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
    fi
    """.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        hdfs_data_dir=get_table_info(0)[1] + '/country_code=nal/dt={{ds}}'
    ),
    dag=dag)

new_driver_cohort_d_touchz_success = BashOperator(

    task_id='new_driver_cohort_d_touchz_success',

    bash_command="""
    line_num=`$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk '{{print $1}}'`

    if [ $line_num -eq 0 ]
    then
        echo "FATAL {hdfs_data_dir} is empty"
        exit 1
    else
        echo "DATA EXPORT Successed ......"
        $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
    fi
    """.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        hdfs_data_dir=get_table_info(1)[1] + '/country_code=nal/dt={{ds}}'
    ),
    dag=dag)

act_user_cohort_d_touchz_success = BashOperator(

    task_id='act_user_cohort_d_touchz_success',

    bash_command="""
    line_num=`$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk '{{print $1}}'`

    if [ $line_num -eq 0 ]
    then
        echo "FATAL {hdfs_data_dir} is empty"
        exit 1
    else
        echo "DATA EXPORT Successed ......"
        $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
    fi
    """.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        hdfs_data_dir=get_table_info(2)[1] + '/country_code=nal/dt={{ds}}'
    ),
    dag=dag)

act_driver_cohort_d_touchz_success = BashOperator(

    task_id='act_driver_cohort_touchz_success',

    bash_command="""
    line_num=`$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk '{{print $1}}'`

    if [ $line_num -eq 0 ]
    then
        echo "FATAL {hdfs_data_dir} is empty"
        exit 1
    else
        echo "DATA EXPORT Successed ......"
        $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
    fi
    """.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        hdfs_data_dir=get_table_info(3)[1] + '/country_code=nal/dt={{ds}}'
    ),
    dag=dag)

dependence_dwd_oride_order_base_include_test_di_prev_day_task >> \
sleep_time >> \
create_oride_cohort_mid_d_task >> \
oride_cohort_mid_d_success >> \
app_oride_new_user_cohort_d_task >> \
new_user_cohort_d_touchz_success

dependence_dwd_oride_order_base_include_test_di_prev_day_task >> \
sleep_time >> \
create_oride_cohort_mid_d_task >> \
oride_cohort_mid_d_success >> \
app_oride_new_driver_cohort_d_task >> \
new_driver_cohort_d_touchz_success

dependence_dwd_oride_order_base_include_test_di_prev_day_task >> \
sleep_time >> \
create_oride_cohort_mid_d_task >> \
oride_cohort_mid_d_success >> \
app_oride_act_user_cohort_d_task >> \
act_user_cohort_d_touchz_success

dependence_dwd_oride_order_base_include_test_di_prev_day_task >> \
sleep_time >> \
create_oride_cohort_mid_d_task >> \
oride_cohort_mid_d_success >> \
app_oride_act_driver_cohort_d_task >> \
act_driver_cohort_d_touchz_success
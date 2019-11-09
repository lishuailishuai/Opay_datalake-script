# -*- coding: utf-8 -*-
"""全局运营概览报表+分城市营运概览报表"""
import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors import UFileSensor
from airflow.sensors import WebHdfsSensor
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor
from utils.connection_helper import get_hive_cursor
from datetime import datetime, timedelta
import re
import logging
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from airflow.sensors.hive_partition_sensor import HivePartitionSensor

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

dag = airflow.DAG('app_oride_order_global_operate_overview_d',
                  schedule_interval="30 3 * * *",
                  default_args=args,
                  catchup=False)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 10',
    dag=dag
)

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

dependence_dm_oride_driver_audit_pass_cube_d_task = UFileSensor(
    task_id='dm_oride_driver_audit_pass_cube_d_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dm_oride_driver_audit_pass_cube_d",
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

table_name = "app_oride_order_global_operate_overview_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
        "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "3600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

app_oride_order_global_operate_overview_d_task = HiveOperator(
    task_id='app_oride_order_global_operate_overview_d_task',

    hql='''
        SET hive.exec.parallel=TRUE;
      set hive.exec.dynamic.partition.mode=nonstrict;
        --将数据加载到内存，临时表
        with 
            order_base as (--订单表
            select
                city_id,
                order_id,
                product_id,
                passenger_id,
                driver_id,
                price,
                is_finish,
                is_finished_pay,
                is_valid,--有效单
                is_wet_order,--湿单
                substr(create_time,1,13) as dt_hour,
                dt
        from oride_dw.dwm_oride_order_base_di 
        where dt = '{pt}' 
        ),
        order_finance as (--财务表
            select  --补贴金额 天 业务线
                city_id,
                product_id,
                sum(recharge_amount+reward_amount) as allowance,
                dt
            from oride_dw.dwd_oride_order_finance_df
            where dt = '{pt}' and create_date = '{pt}'
            group by city_id,product_id,dt
        ),
                
        
        city_info as (--计算天气 只算天 ad
            select 
                city_id,
                city_name,
                weather,
                dt
            from oride_dw. dim_oride_city
            where dt = '{pt}' and city_id not in (999001,999008,999002)
        )
        
        --插入数据
        insert overwrite table oride_dw.{table} partition(country_code,dt)
        select
            ph.city_id as city_id_p_h,--城市id(城市/业务线/小时)
            ph.city_name as city_name_p_h ,--城市名
            ph.weather as weather_p_h,--城市天气
            ph.product_id as product_id_p_h,--业务线
            ph.order_cnt_p_h,--下单数量(城市/业务线/小时)
            ph.valid_ord_cnt_p_h,--有效订单量((城市/业务线/小时))
            ph.wet_ord_cnt_p_h, --湿单订单量(城市/业务线/小时)
            ph.finish_order_cnt_p_h,--完单数量(城市/业务线/小时)
            ph.pay_order_cnt_p_h,--支付完单数量(城市/业务线/小时)
            ph.order_users_p_h,--下单乘客数(城市/业务线/小时)
            ph.finish_order_users_p_h,--完单乘客数(城市/业务线/小时)
            ph.pay_order_users_p_h,--支付乘客数(城市/业务线/小时)
            ph.dt_hour as dt_hour_p_h,--小时(城市/业务线/小时)
        
            ah.city_id as city_id_a_h,--订单id(城市/不分业务线/小时)
            ah.city_name as city_name_a_h,
            ah.weather as weather_a_h,
            ah.order_cnt_a_h,--下单数量(城市/不分业务线/小时)
            ah.valid_ord_cnt_a_h,--有效订单量(城市/不分业务线/小时)
            ah.wet_ord_cnt_a_h,--湿单订单量(城市/不分业务线/小时)
            ah.finish_order_cnt_a_h,--完单数量(城市/不分业务线/小时)
            ah.pay_order_cnt_a_h,--支付完单数量(城市/不分业务线/小时)
            ah.order_users_a_h,--下单乘客数(城市/不分业务线/小时)
            ah.finish_order_users_a_h,--完单乘客数(城市/不分业务线/小时)
            ah.pay_order_users_a_h,--支付乘客数(城市/不分业务线/小时)
            ah.dt_hour as dt_hour_a_h,--小时(城市/不分业务线/小时)
            
            pd.city_id as city_id_p_d,--订单id(城市/业务线/天)
            pd.city_name city_name_p_d,
            pd.weather as weather_p_d,
            pd.product_id as product_id_p_d,--业务线(城市/业务线/天)
            pd.order_cnt_p_d,--订单数量 (城市/业务线/天)
            pd.valid_ord_cnt_p_d, --有效订单数业 (城市/业务线/天)
            pd.wet_ord_cnt_p_d,--湿单订单量 (城市/业务线/天)
            pd.finish_order_cnt_p_d,--完单数量 (城市/业务线/天)
            pd.pay_order_cnt_p_d,--支付完单数量 (城市/业务线/天)
            pd.order_users_p_d,--下单乘客数 (城市/业务线/天)
            pd.finish_order_users_p_d,--完单乘客数 (城市/业务线/天)
            pd.pay_order_users_p_d,  --支付乘客数 (城市/业务线/天)
            pd.allowance_p_d,--补贴金额 业务线/天
            pd.finish_gmv_p_d,--完单gmv 业务线/天
            pd.finish_order_driver_p_d,--完单司机数 业务线/天
            
            ad.city_id as city_id_a_d,
            ad.city_name as city_name_a_d,
            ad.weather as weather_a_d,
            ad.order_cnt_a_d,--订单数量  (城市/不分业务线/天)
            ad.valid_ord_cnt_a_d,--有效订单数业  (城市/不分业务线/天)
            ad.wet_ord_cnt_a_d, --湿单订单量  (城市/不分业务线/天)
            ad.finish_order_cnt_a_d,--完单数量  (城市/不分业务线/天)
            ad.pay_order_cnt_a_d,--支付完单数量  (城市/不分业务线/天)    
            ad.order_users_a_d,--下单乘客数  (城市/不分业务线/天)
            ad.finish_order_users_a_d,  --完单乘客数  (城市/不分业务线/天)
            ad.pay_order_users_a_d,  --支付乘客数  (城市/不分业务线/天)
            ad.allowance_a_d,--补贴金额 (城市/不分业务线/天)
            ad.finish_gmv_a_d,--完单gmv  (城市/不分业务线/天)
            ad.finish_order_driver_a_d,--完单司机数  (城市/不分业务线/天)
            act.act_users as act_users_a_d,--活跃用户数 (城市/不分业务线/天)
            online_driver.td_online_driver_num as online_driver_num_a_d, -- 在线司机数 (城市/不分业务线/天)
            first_ord.open_date,-- 开城日期
            
            'nal' as country_code,--国家二维码
            '{pt}' as dt
        from
        ------------小时数据----------------
        (--城市/业务线/小时
            select
                ord.city_id,
                city_info.city_name,
                city_info.weather,
                ord.product_id,
                count(ord.order_id) as order_cnt_p_h,--订单数量 业务线/小时
                count(if(ord.is_valid=1,ord.order_id,null)) as valid_ord_cnt_p_h, --有效订单数业务线/小时
                count(if(ord.is_wet_order=1,ord.order_id,null)) as wet_ord_cnt_p_h, --湿单订单量 业务线/小时
                sum(ord.is_finish) AS finish_order_cnt_p_h,--完单数 业务线/小时
                sum(ord.is_finished_pay) AS pay_order_cnt_p_h,--支付完单数量 业务线/小时
                count(distinct ord.passenger_id) as order_users_p_h,--下单乘客数
                count( distinct if(ord.is_finish = 1,ord.passenger_id,0)) as finish_order_users_p_h,  --完单乘客数,
                count(distinct if(ord.is_finished_pay = 1,ord.passenger_id,0)) as pay_order_users_p_h,  --支付乘客数,
                ord.dt_hour,
                ord.dt
            from order_base as ord
            left join city_info on ord.city_id = city_info.city_id
            group by
                ord.city_id,
                ord.product_id,
                city_info.city_name,
                city_info.weather,
                ord.dt_hour,
                ord.dt
        )ph
        left join 
        (--城市/不分业务线/小时
            select
                ord.city_id,
                city_info.city_name,
                city_info.weather,
                count(ord.order_id) as order_cnt_a_h,--订单数量 业务线/小时
                count(if(ord.is_valid=1,ord.order_id,null)) as valid_ord_cnt_a_h, --有效订单数业务线/小时
                count(if(ord.is_wet_order=1,ord.order_id,null)) as wet_ord_cnt_a_h, --湿单订单量 业务线/小时
                sum(ord.is_finish) AS finish_order_cnt_a_h,--完单数 业务线/小时
                sum(ord.is_finished_pay) AS pay_order_cnt_a_h,--支付完单数量 业务线/小时
                count(distinct ord.passenger_id) as order_users_a_h,--下单乘客数
                count( distinct if(ord.is_finish = 1,ord.passenger_id,0)) as finish_order_users_a_h,  --完单乘客数,
                count(distinct if(ord.is_finished_pay = 1,ord.passenger_id,0)) as pay_order_users_a_h,  --支付乘客数,
                ord.dt_hour,
                ord.dt
            from order_base as ord
            left join city_info on ord.city_id = city_info.city_id
            group by
                ord.city_id,
                city_info.city_name,
                city_info.weather,
                ord.dt_hour,
                ord.dt
        )ah on  ph.city_id = ah.city_id and ph.dt_hour = ah.dt_hour
        ------------天级别的数据------------
        full outer join 
        (--城市/业务线/天
            select
                ord.city_id,
                city_info.city_name,
                city_info.weather,
                ord.product_id,
            
                count(ord.order_id) as order_cnt_p_d,--订单数量 业务线/天
                count(if(ord.is_valid=1,ord.order_id,null)) as valid_ord_cnt_p_d, --有效订单数业 业务线/天
                count(if(ord.is_wet_order=1,ord.order_id,null)) as wet_ord_cnt_p_d, --湿单订单量 业务线/天
                sum(ord.is_finish) AS finish_order_cnt_p_d,--完单数量 业务线/天
                sum(ord.is_finished_pay) AS pay_order_cnt_p_d,--支付完单数量 业务线/天
            
                count(distinct ord.passenger_id) as order_users_p_d,--下单乘客数 业务线/天
                count( distinct if(ord.is_finish = 1,ord.passenger_id,0)) as finish_order_users_p_d,  --完单乘客数 业务线/天
                count(distinct if(ord.is_finished_pay = 1,ord.passenger_id,0)) as pay_order_users_p_d,  --支付乘客数 业务线/天
                finance_ord.allowance as allowance_p_d, --补贴金额 业务线/天
                sum(if(ord.is_finish = 1,ord.price,0.0)) as finish_gmv_p_d,  --完单gmv 业务线/天
                count( distinct if(ord.is_finish = 1,ord.driver_id,0)) as finish_order_driver_p_d, --完单司机数 业务线/天
                
                'all_day' as  dt_hour,
                ord.dt
                
            from order_base as ord
            left join
            (--计算补贴金额  
                select *
                from order_finance
            )finance_ord on finance_ord.city_id = ord.city_id and finance_ord.product_id = ord.product_id
            left join city_info on ord.city_id = city_info.city_id
            group by
                ord.city_id,
                city_info.city_name,
                city_info.weather,
                ord.product_id,
                finance_ord.allowance,
                'all_day',
                ord.dt
        )pd on ph.city_id = pd.city_id and pd.product_id = ph.product_id and pd.dt = ph.dt and pd.dt_hour = ph.dt_hour
        
        full outer join
        (--城市/不分业务线/天
            select
                ord.city_id,
                city_info.city_name,
                city_info.weather,
                9999  as product_id,
                count(ord.order_id) as order_cnt_a_d,--订单数量 业务线/天
                count(if(ord.is_valid=1,ord.order_id,null)) as valid_ord_cnt_a_d, --有效订单数业 业务线/天
                count(if(ord.is_wet_order=1,ord.order_id,null)) as wet_ord_cnt_a_d, --湿单订单量 业务线/天
                sum(ord.is_finish) AS finish_order_cnt_a_d,--完单数量 业务线/天
                sum(ord.is_finished_pay) AS pay_order_cnt_a_d,--支付完单数量 业务线/天
            
                count(distinct ord.passenger_id) as order_users_a_d,--下单乘客数 业务线/天
                count( distinct if(ord.is_finish = 1,ord.passenger_id,0)) as finish_order_users_a_d,  --完单乘客数 业务线/天
                count(distinct if(ord.is_finished_pay = 1,ord.passenger_id,0)) as pay_order_users_a_d,  --支付乘客数 业务线/天
                finance_ord.allowance as allowance_a_d, --补贴金额 业务线/天
                sum(if(ord.is_finish = 1,ord.price,0.0)) as finish_gmv_a_d,  --完单gmv 业务线/天
                count( distinct if(ord.is_finish = 1,ord.driver_id,0)) as finish_order_driver_a_d, --完单司机数 业务线/天
                
                'all_day' as  dt_hour,
                ord.dt
                
            from order_base as ord
            left join
            (--计算补贴金额  城市天
                select 
                    city_id,
                    dt,
                    sum(allowance) allowance
                from order_finance
                group by city_id,dt
            )finance_ord on finance_ord.city_id = ord.city_id 
            left join city_info on city_info.city_id = ord.city_id
            group by
                ord.city_id,
                city_info.city_name,
                city_info.weather,
                9999,
                finance_ord.allowance,
                'all_day',
                ord.dt
        )ad on ph.city_id = ad.city_id and ph.product_id = ad.product_id and ph.dt = ad.dt and ph.dt_hour = ad.dt_hour

        left join
        (--计算活跃用户  不分业务线 天  不分小时  ad
            SELECT 
                city_id,
                dt,
                count (distinct passenger_id) as act_users
            FROM oride_dw.dim_oride_passenger_base
            WHERE dt= '{pt}' and substr(login_time,1,10) = dt and  city_id <> 0 and city_id < 999000
            group by city_id,dt
        )act on ad.city_id = act.city_id and ad.dt = act.dt
        left join
        (--在线司机数  不分业务线  天 不分小时 ad
                select 
                    city_id,
                    td_online_driver_num
            from oride_dw.dm_oride_driver_audit_pass_cube_d 
            where dt = '{pt}'and city_id >0 and product_id  = -10000
        )online_driver on  online_driver.city_id = ad.city_id
        left join
        (--计算开城日期  ad
            select 
                city_id,
                min(create_date) as open_date
            from dwd_oride_order_base_include_test_df
            where dt in ( '{pt}','his') 
            and status in (4,5)and city_id <> '999001'
            group by city_id 
        )first_ord on ph.city_id = first_ord.city_id;
    '''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
    ),
    schema='oride_dw',
    dag=dag
)


# 生成_SUCCESS
def check_success(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"table": "{dag_name}".format(dag_name=dag_ids),
        "hdfs_path": "{hdfsPath}/country_code=nal/dt={pt}".format(pt=ds, hdfsPath=hdfs_path)}
    ]

    TaskTouchzSuccess().set_touchz_success(msg)


touchz_data_success = PythonOperator(
    task_id='touchz_data_success',
    python_callable=check_success,
    provide_context=True,
    dag=dag
)

dependence_dim_oride_city_task  >>  dependence_dim_oride_passenger_base_task  >> dependence_dwd_oride_order_base_include_test_df_task >>\
dependence_dwd_oride_order_finance_df_task >> dependence_dm_oride_driver_audit_pass_cube_d_task >> dependence_dwm_oride_order_base_di_task >>\
sleep_time >> app_oride_order_global_operate_overview_d_task >> touchz_data_success
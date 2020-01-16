# -*- coding: utf-8 -*-
"""
司机通话记录(二期)埋点表取数
"""
import airflow
from datetime import datetime, timedelta
from airflow.sensors import UFileSensor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.hive_hooks import HiveCliHook
import logging
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from airflow.sensors import OssSensor
from airflow.models import Variable

args = {
    'owner': 'chenghui',
    'start_date': datetime(2019, 12, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_passenger_funnel_second_edition_point_d',
                  schedule_interval="40 01 * * *",
                  default_args=args,
                  catchup=False)
##----------------------------------------- 变量 ---------------------------------------##
db_name = "oride_dw"
table_name = "app_oride_passenger_funnel_second_edition_point_d"

##----------------------------------------- 依赖 ---------------------------------------##
#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":

    dwm_oride_order_base_di_task = UFileSensor(
        task_id='dwm_oride_order_base_di_task',
        filepath='{hdfs_path_str}/country_code=NG/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_order_base_di",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )
    dwd_oride_client_event_detail_hi_task = UFileSensor(
        task_id="dwd_oride_client_event_detail_hi_task",
        filepath='{hdfs_path_str}/country_code=nal/dt={pt}/hour=23/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_client_event_detail_hi",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

else:
    print("成功")
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
    dwd_oride_client_event_detail_hi_task = OssSensor(
        task_id="dwd_oride_client_event_detail_hi_task",
        bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/hour=23/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_client_event_detail_hi",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

# ----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {
            "dag":dag,"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
            "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1200"
        }
    ]
    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def app_oride_passenger_funnel_second_edition_point_d_sql_task(ds):
    HQL = '''
    
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        with base as(
            select *
            from oride_dw.dwd_oride_client_event_detail_hi 
            where dt='{pt}'
            and from_unixtime(cast(event_time as int),'yyyy-MM-dd')='{pt}'
            and user_id>0
        )
        
        insert overwrite table {db}.{table} partition(country_code,dt)
        
        select client_count.choose_end_cnt,--选择终点次数
            client_count.valuation_cnt, --估价订单数
            client_count.choose_end_num,--选择终点人数
            client_count.valuation_num,--估价人数
            order_count.order_cnt, --下单数(成功发出的订单数)
            client_dur.login_choose_end_dur,--登陆-选择终点时长
            client_dur.valuation_order_dur,--估价-下单时长
            order_count_lfw.submit_order_lfw_cnt,--下单数(四周均值)
            client_count_lfw.choose_end_lfw_cnt,--选择终点次数(四周均值)
            client_count_lfw.valuation_lfw_cnt,--估价次数(四周均值)
            client_count_lfw.choose_end_lfw_num,--选择终点人数(四周均值)
            client_count_lfw.valuation_lfw_num,--估价人数(四周均值)
            client_count.order_point_cnt,--下单数(埋点)
            'nal' as country_code,
            '{pt}' as dt
        from(
            select 
                count(if(event_name='choose_end_point_click',1,null)) as choose_end_cnt,--选择终点次数
                count(if(event_name='request_a_ride_show',1,null)) as valuation_cnt,--估价次数
                count(if(event_name='request_a_ride_click',1,null)) as order_point_cnt,--下单次数(埋点)
                count(distinct if(event_name='choose_end_point_click',user_id,null)) as choose_end_num,--选择终点人数
                count(distinct if(event_name='request_a_ride_show',user_id,null)) as valuation_num,--估价人数
                '{pt}' as dt
            from (
                select user_id,event_name,event_time
                from base
                where event_name in('choose_end_point_click','request_a_ride_show','request_a_ride_click') 
                group by user_id,event_name,event_time
            )as client_event
        ) as client_count
        left join
        (
            select count(order_id) as order_cnt,
                '{pt}' as dt
            from oride_dw.dwm_oride_order_base_di 
            where dt='{pt}'
        ) as order_count
        on client_count.dt=order_count.dt
        left join
        (
            select 
                avg(choose_end_time-login_time) as login_choose_end_dur,--登陆-选择终点地址时长
                avg(click_order_time-valuation_time) as valuation_order_dur, --估价-下单时长
                '{pt}' as dt
            from(
                select a.user_id,
                    min(case when b.event_name='oride_show' then b.event_time end) as login_time,--登陆时间
                    min(case when b.event_name='choose_end_point_click' then b.event_time end) as choose_end_time,--选择终点时间
                    min(case when b.event_name='request_a_ride_show' then b.event_time end) as valuation_time,--估价时间
                    min(case when b.event_name='request_a_ride_click' then b.event_time end) as click_order_time --下单时间
                from(
                    select user_id,
                        min(event_time) as evrnt_time_a
                    from base 
                    where get_json_object(event_value, '$.order_id')>0
                    group by user_id
                )a
                left join 
                (
                    select user_id,event_name,event_time
                    from base
                    where event_name in('oride_show','choose_end_point_click','request_a_ride_show','request_a_ride_click')
                )b
                on a.user_id=b.user_id
                where a.evrnt_time_a>b.event_time 
                group by a.user_id
            )c
            where choose_end_time-login_time>0 
                and choose_end_time-login_time<60
                and click_order_time-valuation_time>0 
                and click_order_time-valuation_time<60
        )as client_dur
        on client_count.dt=client_dur.dt
        left join
        (
            SELECT 
                count(order_id)/4 as submit_order_lfw_cnt, --下单量(四周均值)
                '{pt}' as dt
            from oride_dw.dwm_oride_order_base_di
            WHERE datediff('{pt}',dt)>0 
            and datediff('{pt}',dt)<=28
            and from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')
                =from_unixtime(unix_timestamp('{pt}','yyyy-MM-dd'),'u')
        ) order_count_lfw
        on client_count.dt=order_count_lfw.dt
        left join
        (
            select 
                count(if(event_name='choose_end_point_click',1,null))/4 as choose_end_lfw_cnt,--选择终点次数(四周均值)
                count(if(event_name='request_a_ride_show',1,null))/4 as valuation_lfw_cnt,--估价次数(四周均值)
                count(distinct if(event_name='choose_end_point_click',user_id,null))/4 as choose_end_lfw_num,--选择终点次数
                count(distinct if(event_name='request_a_ride_show',user_id,null))/4 as valuation_lfw_num,--估价次数
                '{pt}' as dt
            from (
                select user_id,event_name,event_time
                from oride_dw.dwd_oride_client_event_detail_hi 
                where datediff('{pt}',dt)>0 
                and datediff('{pt}',dt)<=28
                and from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')
                    =from_unixtime(unix_timestamp('{pt}','yyyy-MM-dd'),'u')
                and from_unixtime(cast(event_time as int),'yyyy-MM-dd')=dt
                and user_id>0 
                and event_name in('choose_end_point_click','request_a_ride_show')
                and concat_ws(":",dt,`hour`) != '2019-12-18:15' --垃圾文件 
                group by user_id,event_name,event_time
            )as client_event_lfw
        )as client_count_lfw
        on client_count.dt=client_count_lfw.dt;
    
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
    _sql = app_oride_passenger_funnel_second_edition_point_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)
    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


app_oride_passenger_funnel_second_edition_point_d_task = PythonOperator(
    task_id='app_oride_passenger_funnel_second_edition_point_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwm_oride_order_base_di_task >> app_oride_passenger_funnel_second_edition_point_d_task
dwd_oride_client_event_detail_hi_task >> app_oride_passenger_funnel_second_edition_point_d_task
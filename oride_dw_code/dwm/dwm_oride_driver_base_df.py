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
from airflow.sensors import OssSensor
from airflow.sensors.s3_key_sensor import S3KeySensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2019, 11, 3),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwm_oride_driver_base_df',
                  schedule_interval="40 00 * * *",
                  default_args=args)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dwm_oride_driver_base_df"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 依赖 ---------------------------------------##
dim_oride_driver_base_prev_day_task = OssSensor(
        task_id='dim_oride_driver_base_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dim_oride_driver_base/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
)

# 依赖前一天分区
oride_driver_timerange_prev_day_task = OssSensor(
        task_id='oride_driver_timerange_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw_ods/ods_log_oride_driver_timerange",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
)

dwd_oride_order_base_include_test_di_prev_day_task = OssSensor(
        task_id='dwd_oride_order_base_include_test_di_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

dwd_oride_order_push_driver_detail_di_prev_day_task = OssSensor(
        task_id='dwd_oride_order_push_driver_detail_di_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_order_push_driver_detail_di/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
)

dwd_oride_driver_accept_order_show_detail_di_prev_day_task = OssSensor(
        task_id='dwd_oride_driver_accept_order_show_detail_di_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_accept_order_show_detail_di/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
)

dwd_oride_driver_accept_order_click_detail_di_prev_day_task = OssSensor(
        task_id='dwd_oride_driver_accept_order_click_detail_di_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_accept_order_click_detail_di/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
)

dwm_oride_driver_base_df_prev_day_task = OssSensor(
        task_id='dwm_oride_driver_base_df_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_driver_base_df/country_code=nal",
            pt='{{macros.ds_add(ds, -1)}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
)

dwd_oride_driver_records_day_df_task = OssSensor(
        task_id='dwd_oride_driver_records_day_df_task',
        bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_records_day_df",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
)

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"dag":dag,"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def dwm_oride_driver_base_df_sql_task(ds):
    HQL = '''
   set hive.exec.parallel=true;
   set hive.exec.dynamic.partition.mode=nonstrict;

   with 
    first_data_order as(
        select
            driver_id,
            order_id,
            create_time,--订单时间
            dt,
            min_create_time  as first_finish_create_time--首单时间
        from
        (--相同id 取最大
            select
                driver_id,
                order_id,
                create_time,
                dt,
                min_create_time,
                row_number() over(partition by driver_id order by order_id desc) rn 
    
            from
            (--首单基础信息
                select
                    driver_id,
                    order_id,
                    create_time,
                    dt,
                    min(create_time)  over(partition by driver_id)  as min_create_time--首次完单时间
                from oride_dw.dwd_oride_order_base_include_test_di
                where dt = '{pt}' and is_td_finish =1
                AND city_id<>'999001' --去除测试数据
                    and driver_id<>1
            )t
        )t_first
        where t_first.rn = 1 
    ), 

    recent_data_order as(
        select
            driver_id,
            order_id,
            create_time,
            dt,
            max_create_time as recent_finish_create_time--最近一次完单时间
        from
        (--相同id 取最大
            select
                driver_id,
                order_id,
                create_time,
                dt,
                max_create_time,
                row_number() over(partition by driver_id order by order_id desc) rn 
            from
            (--最近一次完单基础信息
                select
                    driver_id,
                    order_id,
                    create_time,
                    dt,
                    max(create_time)  over(partition by driver_id)  as max_create_time--最近一次完单时间
                from oride_dw.dwd_oride_order_base_include_test_di
                where dt = '{pt}' and is_td_finish =1
                AND city_id<>'999001' --去除测试数据
                    and driver_id<>1
            )t
        )t_recent
        where t_recent.rn = 1 
    ), 

    driver_time as(

        select 
            driver_id,
                driver_freerange,
                driver_onlinerange,
                dt,
                first_online_dt,--首次在线日期
                if(dt = first_online_dt ,1 ,0) as driver_first_online_mark --首次在线日期标志
        from
        (
            SELECT 
                driver_id,
                driver_freerange,
                driver_onlinerange,
                dt,
                min(dt) over(partition by driver_id) as first_online_dt--首次在线日期
            FROM oride_dw_ods.ods_log_oride_driver_timerange
            WHERE dt ='{pt}'
            and driver_id <> 1
        )t
    )

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
select dri.driver_id, 
            --司机ID

            dri.city_id,
            --城市ID

            dri.product_id,
            --司机绑定的业务类型

            dri.register_time,
            --司机注册时间

            if(substr(dri.register_time,1,10)='{pt}',1,0) as is_td_sign,
            --是否当天签约or注册

            dri.is_bind,
            --是否绑定车辆

            null as is_survival,
            --是否存活司机,逻辑待定

            if(dtr.driver_id is not null,1,0) as is_td_online,
            --当天是否在线

            if(push.driver_id is not null,1,0) as is_td_broadcast,
            --当天是否被播单（push节点）

            if(push.success>=1,1,0) as is_td_succ_broadcast,
            --当天是否被成功播单（push节点）

            if(show.driver_id is not null,1,0) as is_td_accpet_show,
            --当天是否被推送订单（骑手端show节点）

            if(click.driver_id is not null,1,0) as is_td_accpet_click,
            --当天是否应答（骑手端click节点）

            if(ord.is_td_request>=1,1,0) as is_td_request,
            --当天是否接单（应答）或者直接关联订单表不为空也可以判断司机是否接单

            if(ord.is_td_finish>=1,1,0) as is_td_finish,
            -- 当天是否完单

            sum(push.push_times) as push_times,
            --司机被播单总次数（push节点）

            sum(push.succ_push_times) as succ_push_times,
            --成功被播单总次数（push节点）

            sum(show.driver_show_order_times) as driver_show_order_times,
            --成功被推送总次数（骑手端show节点）

            sum(click.driver_click_order_times) as driver_click_order_times,
            -- 应答总次数（骑手端click节点）

            sum(push.push_order_cnt) as push_order_cnt, 
            --司机被播单订单量（push节点）

            sum(push.succ_push_order_cnt) as succ_push_order_cnt, 
            --成功被播单订单量（push节点）

            sum(show.driver_show_order_cnt) as driver_show_order_cnt, 
            --成功被推送订单量（骑手端show节点）

            sum(click.driver_click_order_cnt) as driver_click_order_cnt, 
            --成功应答订单量（骑手端click节点）

            sum(ord.is_td_request) as driver_request_order_cnt,
            --司机接单量（理论和应答量一样）

            sum(ord.is_td_finish) as driver_finish_order_cnt,
            --司机当天完单量

            sum(ord.is_td_finish_pay) as driver_finished_pay_order_cnt,
            --司机支付完单量

            sum(ord.price) as driver_finish_price, 
            --司机完单gmv

            sum(ord.td_billing_dur) as driver_billing_dur,
            --司机计费时长

            sum(ord.td_service_dur) as driver_service_dur,
            --司机服务时长

            sum(ord.td_finish_order_dur) as driver_finished_dur,
            --司机支付完单做单时长（支付跨天可能偏大）

            sum(ord.td_cannel_pick_dur) as driver_cannel_pick_dur,
            --司机当天订单被取消时长,该取消时长包含应答后司机、乘客等各方取消，用于计算司机在线时长

            sum(dtr.driver_freerange) as driver_free_dur,
            --司机空闲时长

            sum(if(ord.is_td_finish>=1,(nvl(ord.td_service_dur,0)+nvl(ord.td_cannel_pick_dur,0)+nvl(dtr.driver_freerange,0)),0)) as driver_finish_online_dur,
            --完单司机在线时长

            sum(if(ord.is_td_finish>=1 and is_strong_dispatch>=1,(nvl(ord.td_service_dur,0)+nvl(ord.td_cannel_pick_dur,0)+nvl(dtr.driver_freerange,0)),0)) as strong_driver_finish_online_dur,
            --强派单完单司机在线时长


            if(yes_dwm_driver.first_finish_order_id is not null  ,yes_dwm_driver.first_finish_order_id,first_finish_order.order_id) as first_finish_order_id,
            --首次完单id

            if(yes_dwm_driver.first_finish_order_id is not null  ,yes_dwm_driver.first_finish_order_create_time,first_finish_order.create_time) as first_finish_order_create_time,
            --首次完单创建时间

            if(yes_dwm_driver.recent_finish_order_id is not null  ,yes_dwm_driver.recent_finish_order_id,recent_finish_order.order_id) as recent_finish_order_id,
            --最近完单id

            if(yes_dwm_driver.recent_finish_order_id is not null  , yes_dwm_driver.recent_finish_create_time, recent_finish_order.create_time) as recent_finish_create_time,
            --最近完单时间

            if(yes_dwm_driver.driver_first_online_dt is not null  ,yes_dwm_driver.driver_first_online_dt,driver_online.first_online_dt) as driver_first_online_dt,
            --司机首次在线时间

            driver_amount.amount_all,--'当日总收入'
            driver_amount.amount_agenter,--'当日骑手份子钱-小老板抽成20%'
            
            dri.fault,--正常0(停运)修理1(停运)无资料2(停运)事故3(停运)扣除4(欠缴)5
            dri.first_bind_time, --初次绑定时间 
            dri.end_service_time,--专车司机结束收份子钱时间
            ord.newest_driver_version, --司机端最新版本（接单）
            dri.group_id, --所属组id 

            dri.country_code as country_code,
            dri.dt as dt

       FROM
            (
                SELECT 
                *
                FROM oride_dw.dim_oride_driver_base
                WHERE dt='{pt}'
            ) dri
            LEFT OUTER JOIN
            (
                SELECT *
                FROM driver_time
                WHERE dt='{pt}'
            ) dtr ON dri.driver_id=dtr.driver_id
            AND dri.dt=dtr.dt
            LEFT OUTER JOIN
            (
                SELECT driver_id, --成功播单司机
                count(distinct order_id) as push_order_cnt, --司机被播单订单量（push节点）
                count(1) as push_times,  --司机被播单总次数（push节点）
                count(distinct (if(success=1,order_id,null))) as succ_push_order_cnt, --成功被播单订单量（push节点）
                sum(if(success=1,1,0)) as succ_push_times,  --成功被播单总次数（push节点）
                sum(success) as success  --用于判断该司机是否被成功播单
                FROM oride_dw.dwd_oride_order_push_driver_detail_di
                WHERE dt='{pt}'
                GROUP BY driver_id
            ) push ON dri.driver_id=push.driver_id
            LEFT OUTER JOIN
            (
                SELECT 
                driver_id,
                count(distinct order_id) driver_show_order_cnt, --成功被推送订单量（骑手端show节点）
                count(1) as driver_show_order_times   --成功被推送总次数（骑手端show节点）
                FROM 
                oride_dw.dwd_oride_driver_accept_order_show_detail_di
                WHERE dt='{pt}'
                GROUP BY driver_id
            ) show on dri.driver_id=show.driver_id            
            LEFT OUTER JOIN
            (
                SELECT 
                driver_id,
                count(distinct order_id) driver_click_order_cnt, --成功应答订单量（骑手端click节点）
                count(1) as driver_click_order_times  -- 应答总次数（骑手端click节点）
                FROM 
                oride_dw.dwd_oride_driver_accept_order_click_detail_di
                WHERE dt='{pt}'
                GROUP BY driver_id
            ) click on dri.driver_id=click.driver_id           
            LEFT OUTER JOIN
            (
                SELECT 
                driver_id,
                sum(is_td_request) as is_td_request, --用于判断司机当天是否接单,该字段为司机当天接单量
                sum(is_td_finish) as is_td_finish,  --用于判断司机是否有完单，该字段为司机当天完单量  
                sum(is_td_finish_pay) as is_td_finish_pay,  --用于判断司机是否有支付完单，该字段为司机当天支付完单量
                sum(if(is_td_finish = 1, price, 0)) as price, --司机完单gmv
                sum(td_billing_dur) as td_billing_dur, --司机计费时长，和司机完单计费时长不一样,有些订单status不是完单，比如4变6的那种
                sum(td_service_dur) as td_service_dur, --司机服务时长
                sum(td_finish_order_dur) as td_finish_order_dur, --司机支付完单做单时长（支付跨天可能偏大）
                sum(td_cannel_pick_dur) as td_cannel_pick_dur, --司机当天订单被取消时长
                sum(is_strong_dispatch) as is_strong_dispatch, --用于判断司机是否有强派单

                count(order_id) as succ_push_order_cnt,  --该字段可以用于对比数据
                max(driver_version) as newest_driver_version --司机端最新版本（接单）

                FROM oride_dw.dwd_oride_order_base_include_test_di
                WHERE dt='{pt}'
                AND city_id<>'999001' --去除测试数据
                and driver_id<>1
                group by driver_id
            ) ord ON dri.driver_id=ord.driver_id 
            left join
            (--首次完单信息
                select 
                    driver_id,
                    order_id,
                    create_time,
                    first_finish_create_time
                from first_data_order 
            )first_finish_order on dri.driver_id = first_finish_order.driver_id
            left join
            (--最近一次完单
                select 
                    driver_id,
                    order_id,
                    create_time,
                    recent_finish_create_time
                from recent_data_order
            )recent_finish_order on dri.driver_id =  recent_finish_order.driver_id

            left join
            (--司机首次在线时间
                select
                    driver_id,
                    first_online_dt,
                    driver_first_online_mark
                from driver_time where driver_first_online_mark  = 1
            )driver_online  on dri.driver_id =  driver_online.driver_id

            left join
            (
                select 
                     driver_id,
                    nvl(amount_all,0) as amount_all,--'当日总收入'
                    nvl(amount_agenter,0) as amount_agenter--'当日骑手份子钱-小老板抽成20%'
                from oride_dw.dwd_oride_driver_records_day_df
                    where dt = '{pt}' and  from_unixtime(day,'yyyy-MM-dd') = '{pt}'
            )driver_amount on dri.driver_id  = driver_amount.driver_id

            left join 
            (
                select
                    driver_id,
                    first_finish_order_id,
                    first_finish_order_create_time,
                    recent_finish_order_id,
                    recent_finish_create_time,
                    driver_first_online_dt
                from
                (  
                    select  
                        driver_id,
                        first_finish_order_id,
                        first_finish_order_create_time,
                        recent_finish_order_id,
                        recent_finish_create_time,
                        driver_first_online_dt,
                        row_number() over(partition by driver_id order by recent_finish_order_id desc) rn 
                    from oride_dw.dwm_oride_driver_base_df
                    where dt = '{bef_yes_day}'
                )t where t.rn = 1
            )yes_dwm_driver on  yes_dwm_driver.driver_id  =  dri.driver_id
           group by dri.driver_id, 
            --司机ID

            dri.city_id,
            --城市ID

            dri.product_id,
            --司机绑定的业务类型

            dri.register_time,

            if(substr(dri.register_time,1,10)='{pt}',1,0),
            --是否当天签约or注册

            dri.is_bind,
            --是否绑定车辆

           -- dri.block,
            --是否存活司机0:存活

            if(dtr.driver_id is not null,1,0),
            --当天是否在线

            if(push.driver_id is not null,1,0),
            --当天是否被播单（push节点）

            if(push.success>=1,1,0),
            --当天是否被成功播单（push节点）

            if(show.driver_id is not null,1,0),
            --当天是否被推送订单（骑手端show节点）

            if(click.driver_id is not null,1,0),
            --当天是否应答（骑手端click节点）

            if(ord.is_td_request>=1,1,0),
            --当天是否接单（应答）或者直接关联订单表不为空也可以判断司机是否接单

            if(ord.is_td_finish>=1,1,0),


            if(yes_dwm_driver.first_finish_order_id is not null  ,yes_dwm_driver.first_finish_order_id,first_finish_order.order_id) ,
            --首次完单id

            if(yes_dwm_driver.first_finish_order_id is not null  ,yes_dwm_driver.first_finish_order_create_time,first_finish_order.create_time) ,
            --首次完单创建时间

            if(yes_dwm_driver.recent_finish_order_id is not null  ,yes_dwm_driver.recent_finish_order_id,recent_finish_order.order_id) ,
            --最近完单id

            if(yes_dwm_driver.recent_finish_order_id is not null  , yes_dwm_driver.recent_finish_create_time, recent_finish_order.create_time) ,
            --最近完单时间

            if(yes_dwm_driver.driver_first_online_dt is not null  ,yes_dwm_driver.driver_first_online_dt,driver_online.first_online_dt),
            --司机首次在线时间
            driver_amount.amount_all,
            driver_amount.amount_agenter,--当日骑手份子钱-小老板抽成20%
            
            dri.fault,
            dri.first_bind_time, --初次绑定时间 
            dri.end_service_time,--专车司机结束收份子钱时间
            ord.newest_driver_version, --司机端最新版本（接单）
            dri.group_id, --所属组id 

            dri.country_code,
            dri.dt

    '''.format(
        pt=ds,
        bef_yes_day=airflow.macros.ds_add(ds, -1),
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )
    return HQL


# 熔断数据，如果数据重复，报错
def check_key_data_task(ds):
    cursor = get_hive_cursor()

    # 主键重复校验
    check_sql = '''
    SELECT count(1)-count(distinct driver_id) as cnt
      FROM {db}.{table}
      WHERE dt='{pt}'
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )

    logging.info('Executing 主键重复校验: %s', check_sql)

    cursor.execute(check_sql)

    res = cursor.fetchone()

    if res[0] > 1:
        flag = 1
        raise Exception("Error The primary key repeat !", res)
        sys.exit(1)
    else:
        flag = 0
        print("-----> Notice Data Export Success ......")

    return flag


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwm_oride_driver_base_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwm_oride_driver_base_df_task = PythonOperator(
    task_id='dwm_oride_driver_base_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dim_oride_driver_base_prev_day_task >> dwm_oride_driver_base_df_task
dwd_oride_order_base_include_test_di_prev_day_task >>dwm_oride_driver_base_df_task
dwd_oride_order_push_driver_detail_di_prev_day_task >>dwm_oride_driver_base_df_task
oride_driver_timerange_prev_day_task >> dwm_oride_driver_base_df_task
dwd_oride_driver_accept_order_show_detail_di_prev_day_task >>dwm_oride_driver_base_df_task
dwd_oride_driver_accept_order_click_detail_di_prev_day_task >>dwm_oride_driver_base_df_task
dwd_oride_driver_records_day_df_task>>dwm_oride_driver_base_df_task
dwm_oride_driver_base_df_prev_day_task>>dwm_oride_driver_base_df_task
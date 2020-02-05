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
from plugins.CountriesPublicFrame import CountriesPublicFrame
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

dag = airflow.DAG('dwm_oride_order_base_di',
                  schedule_interval="40 00 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dwm_oride_order_base_di"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 依赖 ---------------------------------------##
dependence_dwd_oride_order_base_include_test_di_prev_day_task = OssSensor(
        task_id='dwd_oride_order_base_include_test_di_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

dependence_dwd_oride_order_assign_driver_detail_di_prev_day_tesk = OssSensor(
        task_id='dwd_oride_order_assign_driver_detail_di_prev_day_tesk',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_order_assign_driver_detail_di/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

dependence_dwd_oride_order_push_driver_detail_di_prev_day_tesk = OssSensor(
        task_id='dwd_oride_order_push_driver_detail_di_prev_day_tesk',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_order_push_driver_detail_di/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

dependence_dwd_oride_driver_accept_order_show_detail_di_prev_day_task = OssSensor(
        task_id='dwd_oride_driver_accept_order_show_detail_di_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_accept_order_show_detail_di/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

dependence_dwd_oride_driver_accept_order_click_detail_di_prev_day_task = OssSensor(
        task_id='dwd_oride_driver_accept_order_click_detail_di_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_accept_order_click_detail_di/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

# 依赖前一天分区
dependence_dwd_oride_order_mark_df_prev_day_task = OssSensor(
        task_id='dwd_oride_order_mark_df_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_order_mark_df/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
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

def dwm_oride_order_base_di_sql_task(ds):
    HQL = '''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    insert overwrite table {db}.{table} partition(country_code,dt)
    select  ord.order_id,
          ord.city_id,
           --所属城市

           ord.product_id as product_id,
           --订单下单业务类型(0: 专快混合 1:driect[专车] 2: street[快车] 99:招手停)

           ord.driver_serv_type,
           --接单后业务类型，和司机绑定

           from_unixtime(ord.create_time,'yyyy-MM-dd HH:mm:ss') as create_time,
           --下单时间

           null as is_peak,
           --是否高峰

           ord.driver_id,
           --司机ID

           ord.passenger_id,
           --乘客id

           ord.is_td_sys_cancel as is_sys_cancel,
           --是否系统取消

           ord.is_td_after_cancel as is_after_cancel,
           --是否应答后取消

           ord.is_td_passanger_before_cancel as is_passanger_before_cancel,
           --是否应答前乘客取消

           ord.is_td_passanger_after_cancel as is_passanger_after_cancel,
           --是否应答后乘客取消

           ord.is_td_driver_after_cancel as is_driver_after_cancel,
           --是否应答后司机取消

           if(push.order_id is not null,1,0) as is_broadcast,
           --是否播单，这个播单包含播了但是没有成功的

           if(push.success>=1,1,0) as is_succ_broadcast,    
           --是否成功播单（push节点）dm层成功播单量没有限定success=1

           if(show.order_id is not null,1,0) as is_accpet_show,
           --是否推送给骑手（骑手端打点show节点，包含骑手端前端show和后端push show）

           if(click.order_id is not null,1,0) as is_accpet_click,
           --是否应答（骑手端打点click节点）

           ord.is_td_request as is_request,
           --是否接单（应答）

           ord.is_td_finish as is_finish,
           --是否完单

           ord.is_td_finish_pay as is_finished_pay,
           --是否完成支付

           ord.td_take_dur as take_order_dur,
           --应单订单时长

           ord.td_pick_up_dur as pick_up_order_dur,
           -- 当天接驾订单时长

           ord.td_cannel_pick_dur as cannel_pick_order_dur,
           --当天取消接驾订单时长

           ord.td_wait_dur as wait_order_dur,
           --当天等待上车订单时长

           ord.td_billing_dur as billing_order_dur,
           --当天计费订单时长

           ord.td_pay_dur as pay_order_dur,
           --当天支付订单时长（该字段有可能跨天支付导致时长偏大）

           ord.td_service_dur,
           --当天服务时长（秒）arrive_time-take_time

           ord.td_finish_order_dur as finished_order_dur,
           --当天支付完单做单时长（该字段有可能跨天支付导致时长偏大）

           if(ord.arrive_time>0,(ord.arrive_time-ord.create_time),0) as user_order_total_dur,
           --乘客下单到行程结束总时长

           assign.pick_up_distance as pick_up_distance,
           --分配节点接驾总距离（assign节点）暂时没有用到

           assign.order_assigned_cnt as order_assigned_cnt,
           --订单被分配次数（assign节点）暂时没有用到

           push.broadcast_distance, 
           --播单总距离

           push.push_all_times_cnt, 
           --播单总次数

           push.succ_broadcast_distance as succ_broadcast_distance,
           --成功播单总距离（push节点）

           push.succ_push_all_times_cnt as succ_push_all_times,
           --成功播单总次数（push节点）

           if(push1.order_id is not null and push1.driver_id is not null,push1.distance,0) as request_order_distance_inpush,
           --抢单阶段接驾距离(应答)

           if(push1.order_id is not null and push1.driver_id is not null,1,0) as is_td_request_inpush,
           --抢单阶段应答单量，11.11号之前不包含招手停、不包含拼车和包车不走push的部分(应答)，11号之后包含拼车播多单情况

           if(push1.order_id is not null and push1.driver_id is not null and ord.is_td_finish=1,push1.distance,0) as finish_order_distance_inpush,
           --抢单阶段接驾距离(完单)

           if(push1.order_id is not null and push1.driver_id is not null and ord.is_td_finish=1,1,0) as is_td_finish_inpush,
           --抢单阶段完单量，11.11号之前不包含招手停、不包含拼车和包车不走push的部分(完单)，11号之后包含拼车播多单情况

           show.driver_show_times as driver_show_times_cnt,
           --骑手端推送给司机总次数（骑手端show节点）

           click.driver_click_times as driver_click_times_cnt,
           --司机应答总次数（骑手端click节点）

           ord.distance as order_onride_distance,
           --送驾距离

           ord.price as price,
           --gmv

           ord.pay_amount as pay_amount,
           --实际支付金额

           mark_ord.is_valid as is_valid,
           --是否有效订单

           if(ord.pay_mode=2,1,0) as is_opay_pay,
           --是否opay支付

           if(ord.pay_status=1,1,0) as is_succ_pay,
           --是否成功支付。全局运营中支付失败是限定pay_status in(0,2)

           mark_ord.is_wet_order as is_wet_order,
           --是否湿单

           mark_ord.score as score,
           --订单评分

           ord.pax_num as pax_num,
           --乘客数

           ord.is_carpool,
           --是否拼车

           ord.is_chartered_bus,
           --是否包车

           ord.is_carpool_success,
           --是否拼车成功

           if(push1.assign_type=1,1,0) as is_strong_dispatch,
           --是否强派1：是，0:否
           
           if(ord.cancel_reason<>'',1,0) as cancel_feedback,
           --是否有取消反馈
           
           ord.status,
           --订单状态 (0: wait assign, 1: pick up passenger, 2: wait passenger, 3: send passenger, 4: arrive destination, 5: finished, 6: cancel,13:乘客取消待支付)
           
           ord.estimate_price,  --预估价格
           
           if(show.order_id is not null and driver_accept_show_times>0,1,0) as is_driver_accept_show, 
           --骑手端是否被展示（骑手端埋点数据，只是包含骑手端前端show部分）
           
           if(ord.wait_time>0,1,0) as is_arrive_receive_point,
           --司机是否到达接客点
           
           ord.pay_mode,
           --订单支付方式
           
           ord.falsify, 
           --用户罚款
           
           ord.falsify_driver_cancel, 
           --司机罚款
           
           ord.driver_price, 
           --司机价格
           
           ord.tip,
           --小费
           
           ord.surcharge,
           --高速费
           
           ord.pax_insurance_price, 
           --乘客保险费
           
 		   ord.country_code as country_code,

           ord.dt as dt
    FROM
      (
         SELECT *
         FROM oride_dw.dwd_oride_order_base_include_test_di
         WHERE dt = '{pt}'
         AND city_id<>'999001' --去除测试数据
         and driver_id<>1
       ) ord
    LEFT OUTER JOIN
      (
        SELECT  
        if(lower(is_multiple)='true',order_id_multiple,order_id) as order_id,
        count(1) as order_assigned_cnt, --订单被分配次数（计算平均接驾距离使用）
        sum(distance) AS pick_up_distance --接驾总距离
        FROM oride_dw.dwd_oride_order_assign_driver_detail_di   --调度算法assign节点
        WHERE dt='{pt}'
        GROUP BY if(lower(is_multiple)='true',order_id_multiple,order_id)
       ) assign ON ord.order_id=assign.order_id
    LEFT OUTER JOIN
      (
        SELECT 
        if(lower(is_multiple)='true',order_id_multiple,order_id) as order_id,  --播单的订单
        sum(if(success=1,distance,0)) AS succ_broadcast_distance, --成功播单距离
        sum(if(success=1,1,0)) AS succ_push_all_times_cnt, --成功播单总次数，目前算法侧播单阶段平均接驾距离=成功播单距离/成功播单总次数,不关注success=0的
        sum(distance) as broadcast_distance, --播单总距离
        count(1) as push_all_times_cnt, --播单总次数
        sum(success) as success  --用于判断是否成功播单
        FROM oride_dw.dwd_oride_order_push_driver_detail_di   --调度算法push节点
        WHERE dt='{pt}'  
        GROUP BY if(lower(is_multiple)='true',order_id_multiple,order_id)
        ) push ON ord.order_id=push.order_id
    LEFT OUTER JOIN   --抢单阶段的平均接驾距离（应答和完单）
      (
        SELECT 
        if(lower(is_multiple)='true',order_id_multiple,order_id) as order_id,  --成功播单的订单
        driver_id,
        assign_type,
        max(order_round) as max_order_round, --播单最大轮数，要么被抢单了要么超时了
        min(distance) as distance --抢单阶段的接驾距离
        FROM oride_dw.dwd_oride_order_push_driver_detail_di   --调度算法push节点
        WHERE dt='{pt}' and success=1 
        GROUP BY if(lower(is_multiple)='true',order_id_multiple,order_id),driver_id,assign_type
        ) push1 ON ord.order_id=push1.order_id and ord.driver_id=push1.driver_id    
    LEFT OUTER JOIN 
    (
        SELECT 
        order_id,
        count(1) as driver_show_times,   --骑手端推送司机总次数
        sum(if(event_name='accept_order_show',1,0)) as driver_accept_show_times  --骑手端被展示次数
        FROM 
        oride_dw.dwd_oride_driver_accept_order_show_detail_di  --骑手show埋点
        WHERE dt='{pt}'
        GROUP BY order_id
    )  show on ord.order_id = show.order_id
    LEFT OUTER JOIN 
    (
        SELECT 
        order_id,
        count(1) driver_click_times  --司机应答次数,司机点接单次数
        FROM 
        oride_dw.dwd_oride_driver_accept_order_click_detail_di
        WHERE dt='{pt}'
        GROUP BY order_id
    ) click on ord.order_id = click.order_id
    left outer join 
    (
        select * from oride_dw.dwd_oride_order_mark_df 
        where dt='{pt}' and substr(create_time,1,10)='{pt}'
    )  mark_ord on ord.order_id=mark_ord.order_id;
    '''.format(
        pt=ds,
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
    SELECT count(1)-count(distinct order_id) as cnt
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
    cf = CountriesPublicFrame("true", ds, db_name, table_name, hdfs_path, "true", "true")

    # 删除分区
    cf.delete_partition()

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dwm_oride_order_base_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 熔断数据
    check_key_data_task(ds)

    # 生产success
    cf.touchz_success()


dwm_oride_order_base_di_task = PythonOperator(
    task_id='dwm_oride_order_base_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

dependence_dwd_oride_order_base_include_test_di_prev_day_task >>dwm_oride_order_base_di_task
dependence_dwd_oride_order_assign_driver_detail_di_prev_day_tesk >>dwm_oride_order_base_di_task
dependence_dwd_oride_order_push_driver_detail_di_prev_day_tesk >>dwm_oride_order_base_di_task
dependence_dwd_oride_driver_accept_order_show_detail_di_prev_day_task >>dwm_oride_order_base_di_task
dependence_dwd_oride_driver_accept_order_click_detail_di_prev_day_task >>dwm_oride_order_base_di_task
dependence_dwd_oride_order_mark_df_prev_day_task >>dwm_oride_order_base_di_task

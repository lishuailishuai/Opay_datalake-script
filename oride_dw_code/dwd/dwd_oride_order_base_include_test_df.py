# -*- coding: utf-8 -*-
import logging
from datetime import datetime, timedelta

import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors import UFileSensor

from utils.connection_helper import get_hive_cursor

args = {
    'owner': 'chenlili',
    'start_date': datetime(2019, 8, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_order_base_include_test_df',
                  schedule_interval="00 01 * * *",
                  default_args=args,
                  catchup=False)

sleep_time_normal = BashOperator(
    task_id='sleep_id_normal',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

sleep_time_his = BashOperator(
    task_id='sleep_id_his',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
ods_sqoop_base_data_order_df_prev_day_task = HivePartitionSensor(
    task_id="ods_sqoop_base_data_order_df_prev_day_task",
    table="ods_sqoop_base_data_order_df",
    partition="dt='{{ds}}'",
    schema="oride_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
ods_sqoop_base_data_order_expired_df_prev_day_task = HivePartitionSensor(
    task_id="ods_sqoop_base_data_order_expired_df_prev_day_task",
    table="ods_sqoop_base_data_order_expired_df",
    partition="dt='{{ds}}'",
    schema="oride_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
ods_sqoop_base_data_order_payment_df_prev_day_task = HivePartitionSensor(
    task_id="ods_sqoop_base_data_order_payment_df_prev_day_task",
    table="ods_sqoop_base_data_order_payment_df",
    partition="dt='{{ds}}'",
    schema="oride_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
dependence_dwd_oride_order_base_include_test_df_result_task = UFileSensor(
    task_id='dwd_oride_order_base_include_test_df_result_task',
    filepath1='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_df/country_code=nal",
        pt='nal'
    ),
    filepath2='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_df/country_code=nal",
            pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

table_name = "dwd_oride_order_base_include_test_df"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

def insert_sql(pt,ds):
    hql = '''
    SET hive.exec.parallel=TRUE;
    SET hive.exec.dynamic.partition.mode=nonstrict;


    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
    SELECT t1.id AS order_id, --订单号
     nvl(t1.city_id,-999) AS city_id, --城市
     t1.serv_type AS product_id, --订单车辆类型(0: 专快混合 1:driect[专车] 2: street[快车] 99:招手停)
     t1.user_id AS passenger_id, -- 乘客ID
     t1.start_name,
     t1.start_lng,
     t1.start_lat,
     t1.end_name,
     t1.end_lng,
     t1.end_lat,
     t1.duration,
     t1.distance,
     t1.basic_fare,
     t1.dst_fare,
     t1.dut_fare,
     t1.dut_price,
     t1.dst_price,
     t1.price,
     t1.reward,
     t1.driver_id,
     t1.plate_num,
     from_unixtime(t1.take_time,'yyyy-MM-dd hh:mm:ss') as take_time, --接单（应答）时间
    from_unixtime(t1.wait_time,'yyyy-MM-dd hh:mm:ss') as wait_time, --到达接送点时间
    from_unixtime(t1.pickup_time,'yyyy-MM-dd hh:mm:ss') as pickup_time, --接到乘客时间
    from_unixtime(t1.arrive_time,'yyyy-MM-dd hh:mm:ss') as arrive_time, --到达终点时间
    from_unixtime(t1.finish_time,'yyyy-MM-dd hh:mm:ss') as finish_time, --订单（支付）完成时间
    t1.cancel_role, --取消人角色(1: 用户, 2: 司机, 3:系统 4:Admin)
    from_unixtime(t1.cancel_time,'yyyy-MM-dd hh:mm:ss') as cancel_time, --取消时间
    t1.cancel_type, --取消原因类型
    t1.cancel_reason,
    t1.status, --订单状态 (0: wait assign, 1: pick up passenger, 2: wait passenger, 3: send passenger, 4: arrive destination, 5: finished, 6: cancel)
    from_unixtime(t1.create_time,'yyyy-MM-dd hh:mm:ss') as create_time, --创建时间
    t1.fraud, ----是否欺诈(0否1是)
    t1.driver_serv_type, -- 司机服务类型(1: Direct 2:Street )
    t1.refund_before_pay,
    t1.refund_after_pay,
    t1.abnormal, --异常状态(0 否 1 逃单)
    t1.flag_down_phone,
    t1.zone_hash, --所属区域
     t1.updated_at AS updated_time,
     from_unixtime(t1.create_time,'yyyy-MM-dd') AS create_date,
     if(t1.driver_id <> 0,1,0) AS is_request, --是否接单（应答）
    if(t1.status IN(4,5),1,0) AS is_finish, --是否完单
    if(t1.pickup_time <> 0,t1.pickup_time - t1.take_time,0) AS pick_up_dur, --接驾时长（秒）
    if(t1.take_time <> 0,t1.take_time - t1.create_time,0) AS take_dur, --应答时长（秒）
    if(t1.cancel_time>0
       AND t1.take_time>0,t1.cancel_time - t1.take_time,0) AS cannel_pick_dur, --取消接驾时长（秒）
    if(t1.pickup_time>0
       AND t1.wait_time>0,t1.pickup_time - t1.wait_time,0) AS wait_dur, --等待上车时长（秒）
    if(t1.arrive_time>0
       AND t1.take_time > 0,t1.arrive_time - t1.take_time,0) AS service_dur, --服务时长（秒）
    if(t1.arrive_time>0
       AND t1.pickup_time > 0,t1.arrive_time - t1.pickup_time,0) AS billing_dur, --计费时长（秒）
    if(t1.status = 5
       AND t1.finish_time>0
       AND t1.arrive_time > 0,t1.finish_time - t1.arrive_time,0) AS pay_dur, -- 支付时长(秒)
    if(t1.status = 6
       AND t1.cancel_role IN(3,4),1,0) AS is_sys_cancel, -- 是否系统取消
    if(t1.status = 6
       AND t1.driver_id = 0
       AND t1.cancel_role = 1,1,0) AS is_passanger_before_cancel, --是否乘客应答前取消
    if(t1.status = 6
       AND t1.driver_id <> 0
       AND t1.cancel_role = 1,1,0) AS is_passanger_after_cancel, --是否乘客应答后取消
    if(t1.status = 6
       AND t1.driver_id <> 0
       AND t1.cancel_role = 2,1,0) AS is_driver_after_cancel, --是否司机应答后取消
    if(t1.status=5,1,0) AS is_finish_pay, --是否完成支付
    nvl(t2.amount,0) AS pay_amount, --实际支付金额
    nvl(t2.mode,0) AS pay_mode, --支付方式（0: 未知, 1: 线下支付, 2: opay, 3: 余额）
    nvl(t2.coupon_id,0) AS coupon_id, --优惠券ID
    t2.coupon_name, -- 优惠券名称
    t2.coupon_amount, --优惠券金额
    t2.bonus, --使用奖励金
    t2.balance, --使用余额
    t2.opay_amount, --opay支付金额
    t2.reference, -- opay流水号
    t2.currency, -- opay货币类型
    t2.status AS pay_status, -- 订单支付状态
    t2.pay_type, -- 订单支付类型
    t1.tip, -- 小费
    if(t1.status IN(4,5)
       AND t1.arrive_time>0
       AND t1.pickup_time > 0,t1.arrive_time - t1.pickup_time,0) AS finish_billing_dur, --完单计费时长（秒）
    if(t1.driver_id <> 0
       AND t1.take_time > 0
       AND t1.finish_time>0,t1.finish_time - t1.take_time,0) AS finish_order_dur, -- 完成做单时长（秒）
    t1.pax_num,  --乘客数
    'nal' AS country_code,
    '{pt}' AS dt
    FROM
      (SELECT *
       FROM {data_order}
       WHERE dt = '{dt}') t1
    LEFT OUTER JOIN
      (SELECT *
       FROM oride_dw_ods.ods_sqoop_base_data_order_payment_df
       WHERE dt = '{dt}'
         ) t2 ON t1.id=t2.id;

    '''

    if pt == 'his':
        data_order='oride_dw_ods.ods_sqoop_base_data_order_expired_df'
        pt='his'
        dt=ds
    else:
        data_order='oride_dw_ods.ods_sqoop_base_data_order_df'
        pt=ds
        dt=ds
    hql_re=hql.format(
        pt=pt,
        dt=ds,
        data_order=data_order,
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
    )
    return hql_re

dwd_oride_order_base_include_test_his_df_task = HiveOperator(
    task_id='dwd_oride_order_base_include_test_his_df_task',
    hql=insert_sql('his','{{ds}}'),
    schema='oride_dw',
    dag=dag
)

dwd_oride_order_base_include_test_df_task = HiveOperator(
    task_id='dwd_oride_order_base_include_test_df_task',
    hql=insert_sql('normal','{{ds}}'),
    schema='oride_dw',
    dag=dag
)

def check_key_data(ds, **kargs):
    # 主键重复校验
    HQL_DQC = '''
    SELECT count(1) as nm
    FROM
     (SELECT order_id,
             count(1) as cnt
      FROM oride_dw.{table}

      GROUP BY order_id HAVING count(1)>1) t1
    '''.format(
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name
    )

    cursor = get_hive_cursor()
    logging.info('Executing 主键重复校验: %s', HQL_DQC)

    cursor.execute(HQL_DQC)
    res = cursor.fetchone()

    if res[0] > 1:
        raise Exception("Error The primary key repeat !", res)
    else:
        print("-----> Notice Data Export Success ......")


# 主键重复校验
task_check_key_data = PythonOperator(
    task_id='check_data',
    python_callable=check_key_data,
    provide_context=True,
    dag=dag)

# 生成_SUCCESS
touchz_data_success = BashOperator(

    task_id='touchz_data_success',

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
        hdfs_data_dir=hdfs_path + '/country_code=nal/dt={{ds}}'
    ),
    dag=dag)



ods_sqoop_base_data_order_df_prev_day_task >> \
ods_sqoop_base_data_order_payment_df_prev_day_task >> \
sleep_time_normal >> \
dwd_oride_order_base_include_test_df_task

ods_sqoop_base_data_order_expired_df_prev_day_task >> \
ods_sqoop_base_data_order_payment_df_prev_day_task >> \
sleep_time_his >> \
dwd_oride_order_base_include_test_his_df_task

dependence_dwd_oride_order_base_include_test_df_result_task>>task_check_key_data >> \
touchz_data_success

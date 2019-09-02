# -*- coding: utf-8 -*-
"""
oride data_trip数据清洗
"""
import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from datetime import datetime, timedelta

args = {
    'owner': 'wuduo',
    'start_date': datetime(2019, 9, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    # 'email': ['bigdata_dw@opay-inc.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': False,
}

dag = airflow.DAG(
    'dwd_oride_data_trip_detail_df',
    schedule_interval="30 02 * * *",
    default_args=args
)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 60',
    dag=dag
)

hive_ods_table = 'ods_sqoop_base_data_trip_df'
hive_ods_db = 'oride_dw'
"""
/------------------------------- 依赖数据源 --------------------------------/
"""
dependence_ods_sqoop_base_data_trip_df = HivePartitionSensor(
    task_id='dependence_ods_sqoop_base_data_trip_df',
    table=hive_ods_table,
    partition="dt='{{ds}}'",
    schema=hive_ods_db,
    poke_interval=60,                                             # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
"""
/---------------------------------- end ----------------------------------/
"""

hive_dwd_table = 'oride_dw.dwd_oride_data_trip_detail_df'

# 创建dwd表
create_dwd_table_task = HiveOperator(
    task_id='create_dwd_table_task',
    hql='''
        CREATE EXTERNAL TABLE IF NOT EXISTS {table} (
            stroke_id           bigint          comment '行程ID',
            driver_id           bigint          comment '司机ID',
            city_id             bigint          comment '所属城市',
            serv_type           int             comment '订单车辆类型(1:driect 2:street 3:keke)',
            pax_num             int             comment '乘客数量',
            pax_max             int             comment '乘客上限',
            duration            bigint          comment '时间',
            distance            bigint          comment '订单距离',
            price               decimal(38,2)   comment '订单价格',
            reward              decimal(38,2)   comment '司机奖励',
            tip                 decimal(38,2)   comment '小费',
            order_id            bigint          comment '订单号',
            create_time         bigint          comment '创建时间',
            start_time          bigint          comment '开始时间',
            finish_time         bigint          comment '完成时间',
            cancel_time         bigint          comment '取消时间',
            status              int             comment '订单状态 (0: initial, 1: ongoing, 2: finished 3: cancel)',
            pickup_order_id     bigint          comment '接单',
            count_down          bigint          comment '倒计时秒'
        ) 
        PARTITIONED BY (
            `country_code` string COMMENT '二位国家码',  
            `dt` string comment '日期'
        )
        STORED AS ORC 
        LOCATION 'ufile://opay-datalake/oride/oride_dw/dwd_oride_data_trip_detail_df' 
        TBLPROPERTIES("orc.compress"="SNAPPY") 
    '''.format(table=hive_dwd_table),
    schema='oride_dw',
    dag=dag
)

# 清洗数据
cleaning_data_to_dwd_task = HiveOperator(
    task_id='cleaning_data_to_dwd_task',
    hql='''
        SET mapred.job.queue.name=root.users.airflow;
        SET hive.exec.parallel=true;
        SET hive.exec.dynamic.partition=true;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        INSERT OVERWRITE TABLE {table} PARTITION(country_code, dt) 
        SELECT 
            NVL(id, 0),
            NVL(driver_id, 0),
            NVL(city_id, 0),
            NVL(serv_type, 0),
            NVL(pax_num, 0),
            NVL(pax_max, 0),
            NVL(duration, 0),
            NVL(distance, 0),
            NVL(price, 0.00),
            NVL(reward, 0.00),
            NVL(tip, 0.00),
            NVL(CAST(order_id AS bigint), 0),
            NVL(create_time, 0),
            NVL(start_time, 0),
            NVL(finish_time, 0),
            NVL(cancel_time, 0),
            NVL(status, 0),
            NVL(pickup_order_id, 0),
            NVL(count_down, 0), 
            'nal' AS country_code, 
            dt 
        FROM (SELECT 
                *,
                split(replace(replace(order_ids,'[',''),']',''), ',') AS orders 
            FROM {ods_db}.{ods_table} 
            WHERE dt = '{pt}'
            ) AS t 
            LATERAL VIEW posexplode(orders) d AS pos, order_id 
    '''.format(
        table=hive_dwd_table,
        ods_db=hive_ods_db,
        ods_table=hive_ods_table,
        pt='{{ ds }}'
    ),
    schema='oride_dw',
    dag=dag
)

# 生成_SUCCESS
touchz_data_success = BashOperator(
    task_id='touchz_data_success',
    bash_command="""
        line_num=`$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk '{{print $1}}'`

        if [ $line_num -eq 0 ]; then
            echo "FATAL {hdfs_data_dir} is empty"
            exit 1
        else
            echo "DATA EXPORT Successed ......"
            $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
        fi
    """.format(
        pt='{{ ds }}',
        now_day='{{ macros.ds_add(ds, +1) }}',
        hdfs_data_dir='ufile://opay-datalake/oride/oride_dw/dwd_oride_data_trip_detail_df/country_code=nal/dt={{ ds }}'
    ),
    dag=dag
)


dependence_ods_sqoop_base_data_trip_df >> sleep_time
create_dwd_table_task >> sleep_time
sleep_time >> cleaning_data_to_dwd_task >> touchz_data_success

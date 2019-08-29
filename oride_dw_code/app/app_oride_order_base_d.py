# -*- coding: utf-8 -*-
"""
订单指标模型
"""
import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors import UFileSensor
from utils.connection_helper import get_hive_cursor
from datetime import datetime, timedelta
import re
import logging

args = {
    'owner': 'wuduo',
    'start_date': datetime(2019, 8, 25),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_oride_order_base_d',
    schedule_interval="30 04 * * *",
    default_args=args
)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 60',
    dag=dag
)

"""
/------------------------------- 依赖数据源 --------------------------------/
"""
dependence_ods_oride_data_order = UFileSensor(
    task_id='dependence_ods_oride_data_order',
    filepath='{hdfs_path_str}/dt={pt}'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_order",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dependence_ods_data_driver_extend = UFileSensor(
    task_id='dependence_ods_data_driver_extend',
    filepath='{hdfs_path_str}/dt={pt}'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_extend",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)
"""
/---------------------------------- end ----------------------------------/
"""

hive_table = 'oride_dw.app_oride_order_base_d'

create_result_table_task = HiveOperator(
    task_id='create_result_table_task',
    hql='''
        CREATE TABLE IF NOT EXISTS {table} (
            city_id                         bigint          comment '城市ID',
            product_id                      bigint          comment '业务线',
            orders                          bigint          comment '下单数',
            orders_taked                    bigint          comment '有司机接单数',
            take_range                      bigint          comment '总应答时长（完单秒）',
            orders_f                        bigint          comment '完成订单数',
            avg_take_range                  bigint          comment '平均应答时长(完单秒)',
            avg_take_range_tk               bigint          comment '平均应答时长(应答秒)',
            cancel_af_take_us               bigint          comment '乘客应答后取消订单数',
            cancel_af_take_dr               bigint          comment '司机应答后取消订单数',
            cancel_all                      bigint          comment '总取消订单数',
            cancel_sys                      bigint          comment '系统总取消订单数',
            cancel_bf_take_us               bigint          comment '乘客应答前取消订单数',
            cancel_af_take_all              bigint          comment '应答后取消订单总数',
            cancel_af_take_us_range         bigint          comment '乘客应答后取消总时长(秒)',
            cancel_af_take_dr_range         bigint          comment '司机应答后取消总时长(秒)',
            billing_range                   bigint          comment '总计费时长/总服务时长(秒)',
            pick_range                      bigint          comment '总接驾时长(秒)',
            send_dis                        decimal(38,2)   comment '总送驾距离(米)', 
            pay_range                       bigint          comment '总支付时长(秒)',
            payed_orders                    bigint          comment '支付订单数',
            valid_orders                    bigint          comment '有效下单数'
        ) 
        PARTITIONED BY (
            `type` string COMMENT '类型',
            `country_code` string COMMENT '二位国家码',  
            `dt` string comment '日期'
        )
        STORED AS ORC 
        LOCATION 'ufile://opay-datalake/oride/oride_dw/app_oride_order_base_d'
    '''.format(table=hive_table),
    schema='oride_dw',
    dag=dag
)


def drop_partions(*op_args, **op_kwargs):
    dt = op_kwargs['ds']
    cursor = get_hive_cursor()
    sql = '''
        show partitions {hive_table}
    '''.format(hive_table=hive_table)
    cursor.execute(sql)
    res = cursor.fetchall()
    logging.info(res)
    for partition in res:
        prt, = partition
        matched = re.search(r'country_code=(?P<cc>\w+)/dt=(?P<dy>.*)$', prt)
        cc = matched.groupdict().get('cc', 'nal')
        dy = matched.groupdict().get('dy', '')
        if dy == dt:
            hql = '''
                ALTER TABLE {hive_table} DROP IF EXISTS PARTITION (country_code='{cc}', dt = '{dt}')
            '''.format(cc=cc, dt=dt, hive_table=hive_table)
            logging.info(hql)
            cursor.execute(hql)

    cursor.close()


"""
drop_partitons_from_table = PythonOperator(
    task_id='drop_partitons_from_table',
    python_callable=drop_partions,
    provide_context=True,
    dag=dag
)
"""

# 不分城市， 不分业务
count_hql_all = '''
--订单数据(不分业务)
orders_data AS 
(
    SELECT 
        0 AS city_id,
        -1 AS serv_type,
        'nal' AS country_code, 
        dt,
        COUNT(1) AS orders,                                                                                         --下单数
        SUM(IF(driver_id>0, 1, 0)) AS orders_take,                                                                  --有司机接单数
        SUM(IF(status IN (4,5), ABS(take_time-create_time), 0)) AS take_range,                                      --总应答时长（完单秒）
        SUM(IF(status IN (4,5), 1, 0)) AS orders_f,                                                                 --完成订单数
        IF(SUM(IF(status IN (4,5), 1, 0))>0, 
            ROUND(SUM(IF(status IN (4,5), ABS(take_time-create_time), 0))/SUM(IF(status IN (4,5), 1, 0))), 
            0
        ) AS avg_take_range,                                                                                        --平均应答时长(完单秒)
        IF(SUM(IF(driver_id>0,1,0))>0, 
            ROUND(SUM(IF(driver_id>0,ABS(take_time-create_time),0))/SUM(IF(driver_id>0,1,0))), 
            0
        ) AS avg_take_range_tk,                                                                                     --平均应答时长(应答秒)
        SUM(IF(status=6 AND driver_id>0 AND cancel_role=1, 1, 0)) AS cancel_af_take_us,                             --乘客应答后取消订单数
        SUM(IF(status=6 AND driver_id>0 AND cancel_role=2, 1, 0)) AS cancel_af_take_dr,                             --司机应答后取消订单数
        SUM(IF(status=6, 1, 0)) AS cancel_all,                                                                      --总取消订单数
        SUM(IF(status=6 AND cancel_role IN (3,4), 1, 0)) AS cancel_sys,                                             --系统总取消订单数
        SUM(IF(status=6 AND driver_id=0 AND cancel_role=1, 1, 0)) AS cancel_bf_take_us,                             --乘客应答前取消订单数
        SUM(IF(status=6 AND driver_id>0, 1, 0)) AS cancel_af_take_all,                                              --应答后取消订单总数
        SUM(IF(status=6 AND driver_id>0 AND cancel_role=1, ABS(cancel_time-take_time), 0)) AS cancel_af_take_us_range, --乘客应答后取消总时长(秒)
        SUM(IF(status=6 AND driver_id>0 AND cancel_role=2, ABS(cancel_time-take_time), 0)) AS cancel_af_take_dr_range, --司机应答后取消总时长(秒)
        SUM(IF(status IN (4,5), ABS(arrive_time-pickup_time), 0)) AS billing_range,                                 --总计费时长/总服务时长(秒)
        SUM(IF(status IN (4,5), ABS(pickup_time-take_time), 0)) AS pick_range,                                      --总接驾时长(秒)
        SUM(IF(status IN (4,5), distance, 0)) AS send_dis,                                                          --总送驾距离(米)
        SUM(IF(status=5, ABS(finish_time-arrive_time), 0)) AS pay_range,                                            --总支付时长(秒)
        SUM(IF(status=5, 1, 0)) AS payed_orders                                                                     --支付订单数
    FROM oride_dw.ods_sqoop_base_data_order_df 
    WHERE dt = '{pt}' AND 
        from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}'
    GROUP BY dt
),
--有效下单数
valid_orders AS 
(
    SELECT 
        0 AS city_id,
        -1 AS serv_type,
        country_code, 
        dt,
        SUM(IF(status IN (4,5), 1, 
            IF(id=id2, 1, 
                IF(ABS(create_time2-create_time)<=1800 AND 
                    2*asin(sqrt(pow(sin((start_lat*pi()/180.0-start_lat2*pi()/180.0)/2),2) + cos(start_lat*pi()/180.0)*cos(start_lat2*pi()/180.0)*pow(sin((start_lng*pi()/180.0-start_lng2*pi()/180.0)/2),2)))*6378137 <= 1000 AND 
                    2*asin(sqrt(pow(sin((end_lat*pi()/180.0-end_lat2*pi()/180.0)/2),2) + cos(end_lat*pi()/180.0)*cos(end_lat2*pi()/180.0)*pow(sin((end_lng*pi()/180.0-end_lng2*pi()/180.0)/2),2)))*6378137 <= 1000, 0, 1
                )
            )
        )) AS valid_order
    FROM (
        SELECT 
            city_id,
            'nal' AS country_code,
            dt,
            id, 
            user_id, 
            start_lng, 
            start_lat, 
            end_lng, 
            end_lat, 
            create_time,
            status,
            LEAD(create_time,1,create_time) OVER(PARTITION BY user_id ORDER BY create_time) create_time2,
            LEAD(start_lng,1,0) OVER(PARTITION BY user_id ORDER BY create_time) start_lng2,
            LEAD(start_lat,1,0) OVER(PARTITION BY user_id ORDER BY create_time) start_lat2,
            LEAD(end_lng,1,0) OVER(PARTITION BY user_id ORDER BY create_time) end_lng2,
            LEAD(end_lat,1,0) OVER(PARTITION BY user_id ORDER BY create_time) end_lat2,
            LEAD(id,1,id) OVER(PARTITION BY user_id ORDER BY create_time) id2
        FROM oride_dw.ods_sqoop_base_data_order_df 
        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}' 
        ) AS t
    GROUP BY country_code, dt 
)
'''.format(pt='{{ ds }}')

insert_result_to_hive_all = HiveOperator(
    task_id='insert_result_to_hive_all',
    hql='''
        SET mapred.job.queue.name=root.users.airflow;
        SET hive.exec.parallel=true;
        SET hive.exec.dynamic.partition=true;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        SET hive.execution.engine=tez;
        SET hive.mapjoin.hybridgrace.hashtable=false;
        --set hive.vectorized.execution.enabled=false;
        SET hive.vectorized.execution.enabled=true;
        SET hive.vectorized.execution.reduce.enabled=true;
        SET hive.prewarm.enabled=true;
        SET hive.prewarm.numcontainers=16;
        SET hive.tez.auto.reducer.parallelism=true;
        WITH {sql} 
        INSERT OVERWRITE TABLE {table} PARTITION (type, country_code, dt) 
        SELECT 
            NVL(a.city_id, NVL(b.city_id, 0)),
            NVL(a.serv_type, NVL(b.serv_type, 0)),
            NVL(a.orders, 0),
            NVL(a.orders_take, 0),
            NVL(a.take_range, 0),
            NVL(a.orders_f, 0),
            NVL(a.avg_take_range, 0), 
            NVL(a.avg_take_range_tk, 0),
            NVL(a.cancel_af_take_us, 0),
            NVL(a.cancel_af_take_dr, 0),
            NVL(a.cancel_all, 0),
            NVL(a.cancel_sys, 0),
            NVL(a.cancel_bf_take_us, 0),
            NVL(a.cancel_af_take_all, 0),
            NVL(a.cancel_af_take_us_range, 0),
            NVL(a.cancel_af_take_dr_range, 0),
            NVL(a.billing_range, 0),
            NVL(a.pick_range, 0),
            NVL(a.send_dis, 0),
            NVL(a.pay_range, 0),
            NVL(a.payed_orders, 0),
            NVL(b.valid_order, 0),
            'all' AS type,
            NVL(a.country_code, NVL(b.country_code, 'nal')) AS country_code, 
            NVL(a.dt, b.dt) AS dt 
        FROM orders_data AS a 
        FULL OUTER JOIN valid_orders AS b ON b.city_id=a.city_id AND b.serv_type=a.serv_type AND b.country_code=a.country_code AND b.dt=a.dt 
    '''.format(sql=count_hql_all, table=hive_table),
    schema='oride_dw',
    dag=dag
)

# 分城市，不分业务
count_hql_city = '''
--订单数据(不分业务)
orders_data AS 
(
    SELECT 
        city_id,
        -1 AS serv_type,
        'nal' AS country_code, 
        dt,
        COUNT(1) AS orders,                                                                                         --下单数
        SUM(IF(driver_id>0, 1, 0)) AS orders_take,                                                                  --有司机接单数
        SUM(IF(status IN (4,5), ABS(take_time-create_time), 0)) AS take_range,                                      --总应答时长（完单秒）
        SUM(IF(status IN (4,5), 1, 0)) AS orders_f,                                                                 --完成订单数
        IF(SUM(IF(status IN (4,5), 1, 0))>0, 
            ROUND(SUM(IF(status IN (4,5), ABS(take_time-create_time), 0))/SUM(IF(status IN (4,5), 1, 0))), 
            0
        ) AS avg_take_range,                                                                                        --平均应答时长(完单秒)
        IF(SUM(IF(driver_id>0,1,0))>0, 
            ROUND(SUM(IF(driver_id>0,ABS(take_time-create_time),0))/SUM(IF(driver_id>0,1,0))), 
            0
        ) AS avg_take_range_tk,                                                                                     --平均应答时长(应答秒)
        SUM(IF(status=6 AND driver_id>0 AND cancel_role=1, 1, 0)) AS cancel_af_take_us,                             --乘客应答后取消订单数
        SUM(IF(status=6 AND driver_id>0 AND cancel_role=2, 1, 0)) AS cancel_af_take_dr,                             --司机应答后取消订单数
        SUM(IF(status=6, 1, 0)) AS cancel_all,                                                                      --总取消订单数
        SUM(IF(status=6 AND cancel_role IN (3,4), 1, 0)) AS cancel_sys,                                             --系统总取消订单数
        SUM(IF(status=6 AND driver_id=0 AND cancel_role=1, 1, 0)) AS cancel_bf_take_us,                             --乘客应答前取消订单数
        SUM(IF(status=6 AND driver_id>0, 1, 0)) AS cancel_af_take_all,                                              --应答后取消订单总数
        SUM(IF(status=6 AND driver_id>0 AND cancel_role=1, ABS(cancel_time-take_time), 0)) AS cancel_af_take_us_range, --乘客应答后取消总时长(秒)
        SUM(IF(status=6 AND driver_id>0 AND cancel_role=2, ABS(cancel_time-take_time), 0)) AS cancel_af_take_dr_range, --司机应答后取消总时长(秒)
        SUM(IF(status IN (4,5), ABS(arrive_time-pickup_time), 0)) AS billing_range,                                 --总计费时长/总服务时长(秒)
        SUM(IF(status IN (4,5), ABS(pickup_time-take_time), 0)) AS pick_range,                                      --总接驾时长(秒)
        SUM(IF(status IN (4,5), distance, 0)) AS send_dis,                                                          --总送驾距离(米)
        SUM(IF(status=5, ABS(finish_time-arrive_time), 0)) AS pay_range,                                            --总支付时长(秒)
        SUM(IF(status=5, 1, 0)) AS payed_orders                                                                     --支付订单数
    FROM oride_dw.ods_sqoop_base_data_order_df 
    WHERE dt = '{pt}' AND 
        from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}'
    GROUP BY dt, city_id
),
--有效下单数
valid_orders AS 
(
    SELECT 
        city_id,
        -1 AS serv_type,
        country_code, 
        dt,
        SUM(IF(status IN (4,5), 1, 
            IF(id=id2, 1, 
                IF(ABS(create_time2-create_time)<=1800 AND 
                    2*asin(sqrt(pow(sin((start_lat*pi()/180.0-start_lat2*pi()/180.0)/2),2) + cos(start_lat*pi()/180.0)*cos(start_lat2*pi()/180.0)*pow(sin((start_lng*pi()/180.0-start_lng2*pi()/180.0)/2),2)))*6378137 <= 1000 AND 
                    2*asin(sqrt(pow(sin((end_lat*pi()/180.0-end_lat2*pi()/180.0)/2),2) + cos(end_lat*pi()/180.0)*cos(end_lat2*pi()/180.0)*pow(sin((end_lng*pi()/180.0-end_lng2*pi()/180.0)/2),2)))*6378137 <= 1000, 0, 1
                )
            )
        )) AS valid_order
    FROM (
        SELECT 
            city_id,
            'nal' AS country_code,
            dt,
            id, 
            user_id, 
            start_lng, 
            start_lat, 
            end_lng, 
            end_lat, 
            create_time,
            status,
            LEAD(create_time,1,create_time) OVER(PARTITION BY user_id ORDER BY create_time) create_time2,
            LEAD(start_lng,1,0) OVER(PARTITION BY user_id ORDER BY create_time) start_lng2,
            LEAD(start_lat,1,0) OVER(PARTITION BY user_id ORDER BY create_time) start_lat2,
            LEAD(end_lng,1,0) OVER(PARTITION BY user_id ORDER BY create_time) end_lng2,
            LEAD(end_lat,1,0) OVER(PARTITION BY user_id ORDER BY create_time) end_lat2,
            LEAD(id,1,id) OVER(PARTITION BY user_id ORDER BY create_time) id2
        FROM oride_dw.ods_sqoop_base_data_order_df 
        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}' 
        ) AS t
    GROUP BY country_code, dt, city_id 
)
'''.format(pt='{{ ds }}')

insert_result_to_hive_city = HiveOperator(
    task_id='insert_result_to_hive_city',
    hql='''
        SET mapred.job.queue.name=root.users.airflow;
        SET hive.exec.parallel=true;
        SET hive.exec.dynamic.partition=true;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        SET hive.execution.engine=tez;
        SET hive.mapjoin.hybridgrace.hashtable=false;
        --set hive.vectorized.execution.enabled=false;
        SET hive.vectorized.execution.enabled=true;
        SET hive.vectorized.execution.reduce.enabled=true;
        SET hive.prewarm.enabled=true;
        SET hive.prewarm.numcontainers=16;
        SET hive.tez.auto.reducer.parallelism=true;
        WITH {sql} 
        INSERT OVERWRITE TABLE {table} PARTITION (type, country_code, dt) 
        SELECT 
            NVL(a.city_id, NVL(b.city_id, 0)),
            NVL(a.serv_type, NVL(b.serv_type, 0)),
            NVL(a.orders, 0),
            NVL(a.orders_take, 0),
            NVL(a.take_range, 0),
            NVL(a.orders_f, 0),
            NVL(a.avg_take_range, 0), 
            NVL(a.avg_take_range_tk, 0), 
            NVL(a.cancel_af_take_us, 0),
            NVL(a.cancel_af_take_dr, 0),
            NVL(a.cancel_all, 0),
            NVL(a.cancel_sys, 0),
            NVL(a.cancel_bf_take_us, 0),
            NVL(a.cancel_af_take_all, 0),
            NVL(a.cancel_af_take_us_range, 0),
            NVL(a.cancel_af_take_dr_range, 0),
            NVL(a.billing_range, 0),
            NVL(a.pick_range, 0),
            NVL(a.send_dis, 0),
            NVL(a.pay_range, 0),
            NVL(a.payed_orders, 0),
            NVL(b.valid_order, 0),
            'city' AS type,
            NVL(a.country_code, NVL(b.country_code, 'nal')) AS country_code, 
            NVL(a.dt, b.dt) AS dt 
        FROM orders_data AS a 
        FULL OUTER JOIN valid_orders AS b ON b.city_id=a.city_id AND b.serv_type=a.serv_type AND b.country_code=a.country_code AND b.dt=a.dt 
    '''.format(sql=count_hql_city, table=hive_table),
    schema='oride_dw',
    dag=dag
)

# 分城市， 分业务
count_hql_city_type = '''
--订单数据(分业务)
orders_data AS 
(
    SELECT 
        de.city_id,
        de.serv_type,
        de.country_code, 
        de.dt,
        0 AS orders,                                                                                                            --下单数
        SUM(IF(o.driver_id>0, 1, 0)) AS orders_take,                                                                            --有司机接单数
        SUM(IF(o.status IN (4,5), ABS(o.take_time-o.create_time), 0)) AS take_range,                                            --总应答时长（完单秒）
        SUM(IF(o.status IN (4,5), 1, 0)) AS orders_f,                                                                           --完成订单数
        IF(SUM(IF(o.status IN (4,5),1,0))>0, 
            ROUND(SUM(IF(o.status IN (4,5), ABS(o.take_time-o.create_time), 0))/SUM(IF(o.status IN (4,5), 1, 0))), 
            0
        ) AS avg_take_range,                                                                                                    --平均应答时长(完单秒)
        IF(SUM(IF(driver_id>0,1,0))>0, 
            ROUND(SUM(IF(driver_id>0,ABS(take_time-create_time),0))/SUM(IF(driver_id>0,1,0))), 
            0
        ) AS avg_take_range_tk,                                                                                                 --平均应答时长(应答秒)
        SUM(IF(o.status=6 AND o.driver_id>0 AND o.cancel_role=1, 1, 0)) AS cancel_af_take_us,                                   --乘客应答后取消订单数
        SUM(IF(o.status=6 AND o.driver_id>0 AND o.cancel_role=2, 1, 0)) AS cancel_af_take_dr,                                   --司机应答后取消订单数
        0 AS cancel_all,
        SUM(IF(o.status=6 AND o.driver_id>0 AND o.cancel_role IN (3,4), 1, 0)) AS cancel_sys,                                   --系统总取消订单数
        0 AS cancel_bf_take_us,
        SUM(IF(o.status=6 AND o.driver_id>0, 1, 0)) AS cancel_af_take_all,                                                      --应答后取消订单总数
        SUM(IF(o.status=6 AND o.driver_id>0 AND o.cancel_role=1, ABS(o.cancel_time-take_time), 0)) AS cancel_af_take_us_range,  --乘客应答后取消总时长(秒)
        SUM(IF(o.status=6 AND o.driver_id>0 AND o.cancel_role=2, ABS(o.cancel_time-take_time), 0)) AS cancel_af_take_dr_range,  --司机应答后取消总时长(秒)
        SUM(IF(o.status IN (4,5), ABS(o.arrive_time-o.pickup_time), 0)) AS billing_range,                                       --总计费时长/总服务时长(秒)
        SUM(IF(o.status IN (4,5), ABS(o.pickup_time-o.take_time), 0)) AS pick_range,                                            --总接驾时长(秒)
        SUM(IF(o.status IN (4,5), distance, 0)) AS send_dis,                                                                    --总送驾距离(米)
        SUM(IF(o.status=5, ABS(o.finish_time-o.arrive_time), 0)) AS pay_range,                                                  --总支付时长(秒)
        SUM(IF(o.status=5, 1, 0)) AS payed_orders                                                                               --支付订单数
    FROM (SELECT 
            driver_id,
            status,
            take_time,
            create_time,
            cancel_role,
            pickup_time,
            arrive_time,
            distance,
            cancel_time,
            finish_time 
        FROM oride_dw.ods_sqoop_base_data_order_df 
        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}' AND 
            driver_id > 0
        ) AS o 
    JOIN (SELECT 
            id, 
            city_id, 
            serv_type,
            'nal' AS country_code, 
            dt 
        FROM oride_dw.ods_sqoop_base_data_driver_extend_df 
        WHERE dt = '{pt}'
        ) AS de  
    WHERE o.driver_id = de.id
    GROUP BY de.country_code, de.dt, de.city_id, de.serv_type 
)
'''.format(pt='{{ ds }}')

insert_result_to_hive_city_type = HiveOperator(
    task_id='insert_result_to_hive_city_type',
    hql='''
        SET mapred.job.queue.name=root.users.airflow;
        SET hive.exec.parallel=true;
        SET hive.exec.dynamic.partition=true;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        SET hive.execution.engine=tez;
        SET hive.mapjoin.hybridgrace.hashtable=false;
        --set hive.vectorized.execution.enabled=false;
        SET hive.vectorized.execution.enabled=true;
        SET hive.vectorized.execution.reduce.enabled=true;
        SET hive.prewarm.enabled=true;
        SET hive.prewarm.numcontainers=16;
        SET hive.tez.auto.reducer.parallelism=true;
        WITH {sql} 
        INSERT OVERWRITE TABLE {table} PARTITION (type, country_code, dt) 
        SELECT 
            NVL(city_id, 0),
            serv_type,
            NVL(orders, 0),
            NVL(orders_take, 0),
            NVL(take_range, 0),
            NVL(orders_f, 0),
            NVL(avg_take_range, 0), 
            NVL(avg_take_range_tk, 0), 
            NVL(cancel_af_take_us, 0),
            NVL(cancel_af_take_dr, 0),
            NVL(cancel_all, 0),
            NVL(cancel_sys, 0),
            NVL(cancel_bf_take_us, 0),
            NVL(cancel_af_take_all, 0),
            NVL(cancel_af_take_us_range, 0),
            NVL(cancel_af_take_dr_range, 0),
            NVL(billing_range, 0),
            NVL(pick_range, 0),
            NVL(send_dis, 0),
            NVL(pay_range, 0),
            NVL(payed_orders, 0),
            0,
            'serv' AS type,
            NVL(country_code, 'nal') AS country_code, 
            dt AS dt 
        FROM orders_data  
    '''.format(sql=count_hql_city_type, table=hive_table),
    schema='oride_dw',
    dag=dag
)


dependence_ods_oride_data_order >> sleep_time
dependence_ods_data_driver_extend >> sleep_time

create_result_table_task >> sleep_time

sleep_time >> insert_result_to_hive_all
sleep_time >> insert_result_to_hive_city
sleep_time >> insert_result_to_hive_city_type

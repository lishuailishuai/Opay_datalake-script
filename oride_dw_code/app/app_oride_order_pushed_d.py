# -*- coding: utf-8 -*-
"""
订单推送指标模型
"""
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
    'app_oride_order_pushed_d',
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
dependence_dwd_oride_order_push_driver_detail_di = UFileSensor(
    task_id='dependence_dwd_oride_order_push_driver_detail_di',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_push_driver_detail_di",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dependence_dwd_oride_order_assign_driver_detail_di = UFileSensor(
    task_id='dependence_dwd_oride_order_assign_driver_detail_di',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_assign_driver_detail_di",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dependence_dwd_oride_order_dispatch_chose_detail_di = UFileSensor(
    task_id='dependence_dwd_oride_order_dispatch_chose_detail_di',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_dispatch_chose_detail_di",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

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

dependence_dwd_oride_driver_accept_order_click_detail_di = UFileSensor(
    task_id='dependence_dwd_oride_driver_accept_order_click_detail_di',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_driver_accept_order_click_detail_di",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dependence_dwd_oride_driver_accept_order_show_detail_di = UFileSensor(
    task_id='dependence_dwd_oride_driver_accept_order_show_detail_di',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_driver_accept_order_show_detail_di",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

"""
/---------------------------------- end ----------------------------------/
"""

hive_table = 'oride_dw.app_oride_order_pushed_d'

create_result_table_task = HiveOperator(
    task_id='create_result_table_task',
    hql='''
        CREATE TABLE IF NOT EXISTS {table} (
            city_id                         bigint          comment '城市ID',
            product_id                      bigint          comment '业务线',
            avg_pushed_distance             decimal(38,2)   comment '平均播单距离',
            avg_picked_distance_done        decimal(38,2)   comment '平均接驾距离(完单)',
            avg_picked_distance_uscancel    decimal(38,2)   comment '平均用户取消订单接驾距离',
            avg_picked_distance_take        decimal(38,2)   comment '平均接驾距离(应答)',
            drivers_pushed_success          bigint          comment '成功推送司机数',
            drivers_count_pushed            bigint          comment '推送给司机的总次数(成功)sum（每个订单被推送的总次数)',
            drivers_count_pushed_all        bigint          comment '推送给司机的总次数(全部)',
            orders_pushed                   bigint          comment '推单数',
            orders_pushed_success           bigint          comment '成功推送订单数',
            drivers_pushed_round            bigint          comment 'SUM(每个订单每轮被推送的司机数去重)'
        ) 
        PARTITIONED BY (
            `type` string COMMENT '类型',
            `country_code` string COMMENT '二位国家码',  
            `dt` string comment '日期'
        )
        STORED AS ORC 
        LOCATION 'ufile://opay-datalake/oride/oride_dw/app_oride_order_pushed_d'
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
        matched = re.search(r'type=(?P<tp>\w+)/country_code=(?P<cc>\w+)/dt=(?P<dy>.*)$', prt)
        tp = matched.groupdict().get('tp', 'all')
        cc = matched.groupdict().get('cc', 'nal')
        dy = matched.groupdict().get('dy', '')
        if dy == dt:
            hql = '''
                ALTER TABLE {hive_table} DROP IF EXISTS PARTITION (type='{tp}', country_code='{cc}', dt = '{dt}')
            '''.format(tp=tp, cc=cc, dt=dt, hive_table=hive_table)
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

# 全部（不分城市，不分业务）
count_hql = '''
--推单距离 
push_distance_finished AS 
(
    SELECT 
        0 AS city_id,
        -1 AS serv_type,
        dt,
        country_code,
        ROUND(IF(SUM(IF(success=1,1,0))>0,
            SUM(IF(success=1,distance,0))/SUM(IF(success=1,1,0)), 
            0), 
        2) AS avg_pushed_distance,                                                      --平均播单距离 
        COUNT(DISTINCT IF(success=1, driver_id, null)) AS drivers_pushed_success,       --成功推送司机数
        SUM(IF(success=1, 1, 0)) AS drivers_count_pushed,                               --推送给司机的总次数(成功)sum（每个订单被推送的总次数)
        COUNT(1) AS drivers_count_pushed_all,                                           --推送给司机的总次数(全部)
        COUNT(DISTINCT IF(success=1, order_id, null)) AS orders_pushed_success          --成功推送订单数
    FROM oride_dw.dwd_oride_order_push_driver_detail_di 
    WHERE dt = '{pt}' 
    GROUP BY country_code, dt 
), 
--平均接驾距离(完单), 平均用户取消订单接驾距离
pick_distance_finished AS 
(
    SELECT 
        0 AS city_id,
        -1 AS serv_type,
        m.dt,
        m.country_code,
        ROUND(IF(SUM(IF(o.status in (4, 5), 1, 0))>0, 
            SUM(IF(o.status in (4, 5), m.distance, 0))/SUM(IF(o.status in (4, 5), 1, 0)), 
            0), 
        2) AS avg_picked_distance_done,                                  --平均接驾距离(完单)
        ROUND(IF(SUM(IF(o.status=6 AND o.cancel_role=1, 1, 0))>0, 
            SUM(IF(o.status=6 AND o.cancel_role=1, m.distance, 0))/SUM(IF(o.status=6 AND o.cancel_role=1, 1, 0)), 
            0), 
        2) AS avg_picked_distance_uscancel                              --平均用户取消订单接驾距离
    FROM 
        (SELECT 
            dt,
            country_code,
            distance,
            order_id
        FROM oride_dw.dwd_oride_order_assign_driver_detail_di 
        WHERE dt = '{pt}' 
        ) AS m 
    INNER JOIN (SELECT 
            id, 
            status, 
            cancel_role 
        FROM oride_dw_ods.ods_sqoop_base_data_order_df 
        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}'
        ) AS o 
    ON m.order_id = o.id 
    GROUP BY m.country_code, m.dt
),
--平均接驾距离(应答)
pick_distance_taked AS 
(
    SELECT 
        0 AS city_id,
        -1 AS serv_type,
        m.dt,
        m.country_code,
        ROUND(IF(COUNT(1)>0, 
            SUM(m.distance)/COUNT(1), 
            0), 
        2) AS avg_picked_distance_take                                 --平均接驾距离(应答)
    FROM 
        (SELECT 
            dt,
            country_code,
            distance,
            order_id
        FROM oride_dw.dwd_oride_order_assign_driver_detail_di 
        WHERE dt = '{pt}'  
        ) AS m 
    INNER JOIN (SELECT 
            order_id 
        FROM oride_dw.dwd_oride_driver_accept_order_click_detail_di 
        WHERE dt = '{pt}' 
        GROUP BY order_id 
        ) AS o 
    ON m.order_id = o.order_id 
    GROUP BY m.country_code, m.dt
), 
--SUM(每个订单每轮被推送的司机数去重)
dirvers_pushed AS 
(
    SELECT 
        0 AS city_id,
        -1 AS serv_type,
        dt,
        country_code,
        SUM(drivers) AS drivers_pushed_round            --SUM(每个订单每轮被推送的司机数去重)
    FROM 
        (SELECT 
            dt,
            country_code,
            order_id,
            order_round,
            COUNT(DISTINCT driver_id) AS drivers 
        FROM oride_dw.dwd_oride_order_push_driver_detail_di 
        WHERE dt = '{pt}' AND 
            success = 1 
        GROUP BY country_code, dt, order_id, order_round
        ) AS t 
    GROUP BY country_code, dt
),
--推单数
push_data_chose AS 
(
   SELECT 
        0 AS city_id,
        -1 AS serv_type,
        country_code,
        dt,
        COUNT(DISTINCT order_id) AS orders_pushed                    --推单数
    FROM oride_dw.dwd_oride_order_dispatch_chose_detail_di 
    WHERE dt = '{pt}'   
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
            NVL(a.city_id, NVL(b.city_id, NVL(c.city_id, NVL(d.city_id, e.city_id)))),
            NVL(a.serv_type, NVL(b.serv_type, NVL(c.serv_type, NVL(d.serv_type, e.serv_type)))),
            CASE WHEN a.avg_pushed_distance IS NULL THEN 0 ELSE a.avg_pushed_distance END,
            CASE WHEN b.avg_picked_distance_done IS NULL THEN 0 ELSE b.avg_picked_distance_done END,
            CASE WHEN b.avg_picked_distance_uscancel IS NULL THEN 0 ELSE b.avg_picked_distance_uscancel END,
            CASE WHEN c.avg_picked_distance_take IS NULL THEN 0 ELSE c.avg_picked_distance_take END,
            CASE WHEN a.drivers_pushed_success IS NULL THEN 0 ELSE a.drivers_pushed_success END, 
            CASE WHEN a.drivers_count_pushed IS NULL THEN 0 ELSE a.drivers_count_pushed END,
            CASE WHEN a.drivers_count_pushed_all IS NULL THEN 0 ELSE a.drivers_count_pushed_all END,
            CASE WHEN e.orders_pushed IS NULL THEN 0 ELSE e.orders_pushed END,
            CASE WHEN a.orders_pushed_success IS NULL THEN 0 ELSE a.orders_pushed_success END,
            CASE WHEN d.drivers_pushed_round IS NULL THEN 0 ELSE d.drivers_pushed_round END, 
            'all' AS type,
            NVL(a.country_code, NVL(b.country_code, NVL(c.country_code, NVL(d.country_code, e.country_code)))) AS country_code, 
            NVL(a.dt, NVL(b.dt, NVL(c.dt, NVL(d.dt, e.dt)))) AS dt 
        FROM push_distance_finished AS a 
        FULL OUTER JOIN pick_distance_finished AS b ON b.city_id=a.city_id AND b.serv_type=a.serv_type AND b.country_code=a.country_code AND b.dt=a.dt 
        FULL OUTER JOIN pick_distance_taked AS c ON c.city_id=a.city_id AND c.serv_type=a.serv_type AND c.country_code=a.country_code AND c.dt=a.dt 
        FULL OUTER JOIN dirvers_pushed AS d ON d.city_id=a.city_id AND d.serv_type=a.serv_type AND d.country_code=a.country_code AND d.dt=a.dt 
        FULL OUTER JOIN push_data_chose AS e ON e.city_id=a.city_id AND e.serv_type=a.serv_type AND e.country_code=a.country_code AND e.dt=a.dt
    '''.format(sql=count_hql, table=hive_table, pt='{{ ds }}'),
    schema='oride_dw',
    dag=dag
)

# 分城市，不分业务
count_city_hql = '''
--推单距离 
push_distance_finished AS 
(
    SELECT 
        de.city_id,
        -1 AS serv_type,
        sm.country_code,
        sm.dt,
        ROUND(IF(SUM(IF(sm.success=1,1,0))>0,
            SUM(IF(sm.success=1,sm.distance,0))/SUM(IF(sm.success=1,1,0)), 
            0), 
        2) AS avg_pushed_distance,                                                          --平均播单距离
        COUNT(DISTINCT IF(sm.success=1, sm.driver_id, null)) AS drivers_pushed_success,     --成功推送司机数
        SUM(IF(sm.success=1, 1, 0)) AS drivers_count_pushed,                                --推送给司机的总次数(成功)sum（每个订单被推送的总次数)
        COUNT(1) AS drivers_count_pushed_all,                                               --推送给司机的总次数(全部)
        COUNT(DISTINCT IF(success=1, sm.order_id, null)) AS orders_pushed_success           --成功推送订单数 
    FROM (SELECT 
            id,  
            city_id,  
            dt 
        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 
        WHERE dt = '{pt}'
        ) AS de  
    INNER JOIN (SELECT 
            dt,
            country_code,
            success,
            driver_id,
            distance, 
            order_id
        FROM oride_dw.dwd_oride_order_push_driver_detail_di 
        WHERE dt = '{pt}' 
        ) AS sm 
    WHERE sm.driver_id = de.id  
    GROUP BY sm.country_code, sm.dt, de.city_id 
), 
--平均接驾距离(完单), 平均用户取消订单接驾距离
pick_distance_finished AS 
(
    SELECT 
        de.city_id,
        -1 AS serv_type,
        m.country_code,
        m.dt,
        ROUND(IF(SUM(IF(o.status IN (4, 5), 1, 0))>0, 
            SUM(IF(o.status IN (4, 5), m.distance, 0))/SUM(IF(o.status IN (4, 5), 1, 0)), 
            0), 
        2) AS avg_picked_distance_done,                                  --平均接驾距离(完单)
        ROUND(IF(SUM(IF(o.status=6 AND o.cancel_role=1, 1, 0))>0, 
            SUM(IF(o.status=6 AND o.cancel_role=1, m.distance, 0))/SUM(IF(o.status=6 AND o.cancel_role=1, 1, 0)), 
            0), 
        2) AS avg_picked_distance_uscancel                              --平均用户取消订单接驾距离
    FROM 
        (SELECT  
            dt,
            country_code,
            driver_id, 
            distance,
            order_id
        FROM oride_dw.dwd_oride_order_assign_driver_detail_di 
        WHERE dt = '{pt}' 
        ) AS m 
    INNER JOIN (SELECT 
            id, 
            status, 
            cancel_role
        FROM oride_dw_ods.ods_sqoop_base_data_order_df 
        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}'
        ) AS o ON m.order_id = o.id 
    INNER JOIN (SELECT 
            id,  
            city_id 
        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 
        WHERE dt = '{pt}'
        ) AS de ON m.driver_id = de.id  
    GROUP BY m.country_code, m.dt, de.city_id 
),
--平均接驾距离(应答)
pick_distance_taked AS 
(
    SELECT 
        de.city_id,
        -1 AS serv_type,
        m.country_code,
        m.dt,
        ROUND(IF(COUNT(1)>0, 
            SUM(m.distance)/COUNT(1), 
            0), 
        2) AS avg_picked_distance_take                                 --平均接驾距离(应答)
    FROM 
        (SELECT  
            dt,
            country_code,
            driver_id, 
            distance,
            order_id
        FROM oride_dw.dwd_oride_order_assign_driver_detail_di 
        WHERE dt = '{pt}' 
        ) AS m 
    INNER JOIN (SELECT 
            order_id 
        FROM oride_dw.dwd_oride_driver_accept_order_click_detail_di 
        WHERE dt = '{pt}' 
        GROUP BY order_id 
        ) AS o ON m.order_id = o.order_id  
    INNER JOIN (SELECT 
            id, 
            city_id  
        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 
        WHERE dt = '{pt}'
        ) AS de ON m.driver_id = de.id 
    GROUP BY m.country_code, m.dt, de.city_id
),
--推单数
push_data_chose AS 
(
    SELECT 
        de.city_id,
        -1 AS serv_type,
        m.country_code,
        m.dt,
        COUNT(DISTINCT m.order_id) AS orders_pushed                    --推单数
    FROM (SELECT 
            dt,
            country_code, 
            driver_id, 
            order_id
        FROM oride_dw.dwd_oride_order_dispatch_chose_detail_di 
        WHERE dt = '{pt}' 
        ) AS m 
    INNER JOIN (
        SELECT 
            id, 
            city_id 
        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 
        WHERE dt = '{pt}'
        ) AS de 
    WHERE m.driver_id = de.id 
    GROUP BY m.country_code, m.dt, de.city_id
),
--SUM(每个订单每轮被推送的司机数去重)
dirvers_pushed AS 
(
    SELECT 
        t.city_id,
        -1 AS serv_type,
        t.country_code,
        t.dt,
        SUM(drivers) AS drivers_pushed_round                        --SUM(每个订单每轮被推送的司机数去重)
    FROM 
        (SELECT 
            do.city_id,
            sm.country_code,
            sm.dt,
            COUNT(DISTINCT sm.driver_id) AS drivers
        FROM (SELECT 
                dt,
                country_code,
                driver_id,
                order_id,
                order_round 
            FROM oride_dw.dwd_oride_order_push_driver_detail_di  
            WHERE dt = '{pt}' AND 
                success = 1
            ) AS sm 
        INNER JOIN (SELECT 
                id, 
                city_id  
            FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 
            WHERE dt = '{pt}'
            ) AS do  
        WHERE sm.driver_id = do.id  
        GROUP BY sm.country_code, sm.dt, do.city_id, sm.order_id, sm.order_round
        ) AS t 
    GROUP BY t.country_code, t.dt, t.city_id
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
            NVL(a.city_id, NVL(b.city_id, NVL(c.city_id, NVL(d.city_id, e.city_id)))),
            NVL(a.serv_type, NVL(b.serv_type, NVL(c.serv_type, NVL(d.serv_type, e.serv_type)))),
            CASE WHEN a.avg_pushed_distance IS NULL THEN 0 ELSE a.avg_pushed_distance END,
            CASE WHEN b.avg_picked_distance_done IS NULL THEN 0 ELSE b.avg_picked_distance_done END,
            CASE WHEN b.avg_picked_distance_uscancel IS NULL THEN 0 ELSE b.avg_picked_distance_uscancel END,
            CASE WHEN c.avg_picked_distance_take IS NULL THEN 0 ELSE c.avg_picked_distance_take END,
            CASE WHEN a.drivers_pushed_success IS NULL THEN 0 ELSE a.drivers_pushed_success END, 
            CASE WHEN a.drivers_count_pushed IS NULL THEN 0 ELSE a.drivers_count_pushed END,
            CASE WHEN a.drivers_count_pushed_all IS NULL THEN 0 ELSE a.drivers_count_pushed_all END,
            CASE WHEN e.orders_pushed IS NULL THEN 0 ELSE e.orders_pushed END,
            CASE WHEN a.orders_pushed_success IS NULL THEN 0 ELSE a.orders_pushed_success END,
            CASE WHEN d.drivers_pushed_round IS NULL THEN 0 ELSE d.drivers_pushed_round END, 
            'city' AS type,
            NVL(a.country_code, NVL(b.country_code, NVL(c.country_code, NVL(d.country_code, e.country_code)))) AS country_code, 
            NVL(a.dt, NVL(b.dt, NVL(c.dt, NVL(d.dt, e.dt)))) AS dt 
        FROM push_distance_finished AS a 
        FULL OUTER JOIN pick_distance_finished AS b ON b.city_id=a.city_id AND b.serv_type=a.serv_type AND b.country_code=a.country_code AND b.dt=a.dt 
        FULL OUTER JOIN pick_distance_taked AS c ON c.city_id=a.city_id AND c.serv_type=a.serv_type AND c.country_code=a.country_code AND c.dt=a.dt 
        FULL OUTER JOIN dirvers_pushed AS d ON d.city_id=a.city_id AND d.serv_type=a.serv_type AND d.country_code=a.country_code AND d.dt=a.dt 
        FULL OUTER JOIN push_data_chose AS e ON e.city_id=a.city_id AND e.serv_type=a.serv_type AND e.country_code=a.country_code AND e.dt=a.dt
    '''.format(sql=count_city_hql, table=hive_table),
    schema='oride_dw',
    dag=dag
)

# 分城市，分业务
count_city_type_hql = '''
--推单距离 
push_distance_finished AS 
(
    SELECT 
        de.city_id,
        de.serv_type,
        sm.country_code,
        sm.dt,
        ROUND(IF(SUM(IF(sm.success=1,1,0))>0,
            SUM(IF(sm.success=1,sm.distance,0))/SUM(IF(sm.success=1,1,0)), 
            0), 
        2) AS avg_pushed_distance,                                                          --平均播单距离
        COUNT(DISTINCT IF(sm.success=1, sm.driver_id, null)) AS drivers_pushed_success,     --成功推送司机数
        SUM(IF(sm.success=1, 1, 0)) AS drivers_count_pushed,                                --推送给司机的总次数(成功)sum（每个订单被推送的总次数)
        COUNT(1) AS drivers_count_pushed_all,                                               --推送给司机的总次数(全部)
        COUNT(DISTINCT IF(success=1, sm.order_id, null)) AS orders_pushed_success           --成功推送订单数 
    FROM (SELECT 
            id,  
            city_id, 
            serv_type 
        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 
        WHERE dt = '{pt}'
        ) AS de  
    INNER JOIN (SELECT 
            dt, 
            country_code, 
            success,
            driver_id,
            order_id,
            distance 
        FROM oride_dw.dwd_oride_order_push_driver_detail_di 
        WHERE dt = '{pt}' 
        ) AS sm 
    WHERE sm.driver_id = de.id  
    GROUP BY sm.country_code, sm.dt, de.city_id, de.serv_type 
), 
--平均接驾距离(完单), 平均用户取消订单接驾距离
pick_distance_finished AS 
(
    SELECT 
        de.city_id,
        de.serv_type, 
        m.country_code,
        m.dt,
        ROUND(IF(SUM(IF(o.status IN (4, 5), 1, 0))>0, 
            SUM(IF(o.status IN (4, 5), m.distance, 0))/SUM(IF(o.status IN (4, 5), 1, 0)), 
            0), 
        2) AS avg_picked_distance_done,                                  --平均接驾距离(完单)
        ROUND(IF(SUM(IF(o.status=6 AND o.cancel_role=1, 1, 0))>0, 
            SUM(IF(o.status=6 AND o.cancel_role=1, m.distance, 0))/SUM(IF(o.status=6 AND o.cancel_role=1, 1, 0)), 
            0), 
        2) AS avg_picked_distance_uscancel                              --平均用户取消订单接驾距离
    FROM 
        (SELECT  
            dt,
            country_code, 
            driver_id, 
            distance,
            order_id
        FROM oride_dw.dwd_oride_order_assign_driver_detail_di 
        WHERE dt = '{pt}' 
        ) AS m 
    INNER JOIN (SELECT 
            id, 
            status, 
            cancel_role
        FROM oride_dw_ods.ods_sqoop_base_data_order_df 
        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}'
        ) AS o ON m.order_id = o.id 
    INNER JOIN (SELECT 
            id,  
            city_id, 
            serv_type 
        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 
        WHERE dt = '{pt}'
        ) AS de ON m.driver_id = de.id  
    GROUP BY m.country_code, m.dt, de.city_id, de.serv_type 
),
--平均接驾距离(应答)
pick_distance_taked AS 
(
    SELECT 
        de.city_id,
        de.serv_type,
        m.country_code,
        m.dt,
        ROUND(IF(COUNT(1)>0, 
            SUM(m.distance)/COUNT(1), 
            0), 
        2) AS avg_picked_distance_take                                 --平均接驾距离(应答)
    FROM 
        (SELECT 
            dt,
            country_code, 
            driver_id, 
            distance,
            order_id
        FROM oride_dw.dwd_oride_order_assign_driver_detail_di 
        WHERE dt = '{pt}' 
        ) AS m 
    INNER JOIN (SELECT 
            order_id 
        FROM oride_dw.dwd_oride_driver_accept_order_click_detail_di 
        WHERE dt = '{pt}' 
        GROUP BY order_id 
        ) AS o ON m.order_id = o.order_id 
    INNER JOIN (SELECT 
            id, 
            city_id, 
            serv_type 
        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 
        WHERE dt = '{pt}'
        ) AS de ON m.driver_id = de.id  
    GROUP BY m.country_code, m.dt, de.city_id, de.serv_type 
),
--推单数
push_data_chose AS 
(
    SELECT 
        de.city_id,
        de.serv_type,
        m.country_code,
        m.dt,
        COUNT(DISTINCT m.order_id) AS orders_pushed                    --推单数
    FROM (SELECT 
            dt,
            country_code, 
            driver_id, 
            order_id
        FROM oride_dw.dwd_oride_order_dispatch_chose_detail_di 
        WHERE dt = '{pt}' 
        ) AS m 
    INNER JOIN (
        SELECT 
            id, 
            city_id, 
            serv_type  
        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 
        WHERE dt = '{pt}'
        ) AS de 
    WHERE m.driver_id = de.id 
    GROUP BY m.country_code, m.dt, de.city_id, de.serv_type 
),
--SUM(每个订单每轮被推送的司机数去重)
dirvers_pushed AS 
(
    SELECT 
        t.city_id,
        t.serv_type,
        t.country_code,
        t.dt,
        SUM(drivers) AS drivers_pushed_round                        --SUM(每个订单每轮被推送的司机数去重)
    FROM 
        (SELECT 
            do.city_id,
            do.serv_type, 
            sm.country_code,
            sm.dt,
            COUNT(DISTINCT sm.driver_id) AS drivers
        FROM (SELECT 
                dt,
                country_code, 
                driver_id,
                order_id,
                order_round 
            FROM oride_dw.dwd_oride_order_push_driver_detail_di 
            WHERE dt = '{pt}' AND 
                success = 1
            ) AS sm 
        INNER JOIN (SELECT 
                id, 
                city_id, 
                serv_type  
            FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 
            WHERE dt = '{pt}'
            ) AS do  
        WHERE sm.driver_id = do.id  
        GROUP BY sm.country_code, sm.dt, do.city_id, do.serv_type, sm.order_id, sm.order_round
        ) AS t 
    GROUP BY t.country_code, t.dt, t.city_id, t.serv_type 
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
            NVL(a.city_id, NVL(b.city_id, NVL(c.city_id, NVL(d.city_id, e.city_id)))),
            NVL(a.serv_type, NVL(b.serv_type, NVL(c.serv_type, NVL(d.serv_type, e.serv_type)))),
            CASE WHEN a.avg_pushed_distance IS NULL THEN 0 ELSE a.avg_pushed_distance END,
            CASE WHEN b.avg_picked_distance_done IS NULL THEN 0 ELSE b.avg_picked_distance_done END,
            CASE WHEN b.avg_picked_distance_uscancel IS NULL THEN 0 ELSE b.avg_picked_distance_uscancel END,
            CASE WHEN c.avg_picked_distance_take IS NULL THEN 0 ELSE c.avg_picked_distance_take END,
            CASE WHEN a.drivers_pushed_success IS NULL THEN 0 ELSE a.drivers_pushed_success END, 
            CASE WHEN a.drivers_count_pushed IS NULL THEN 0 ELSE a.drivers_count_pushed END,
            CASE WHEN a.drivers_count_pushed_all IS NULL THEN 0 ELSE a.drivers_count_pushed_all END,
            CASE WHEN e.orders_pushed IS NULL THEN 0 ELSE e.orders_pushed END,
            CASE WHEN a.orders_pushed_success IS NULL THEN 0 ELSE a.orders_pushed_success END,
            CASE WHEN d.drivers_pushed_round IS NULL THEN 0 ELSE d.drivers_pushed_round END, 
            'serv' AS type,
            NVL(a.country_code, NVL(b.country_code, NVL(c.country_code, NVL(d.country_code, e.country_code)))) AS country_code, 
            NVL(a.dt, NVL(b.dt, NVL(c.dt, NVL(d.dt, e.dt)))) AS dt 
        FROM push_distance_finished AS a 
        FULL OUTER JOIN pick_distance_finished AS b ON b.city_id=a.city_id AND b.serv_type=a.serv_type AND b.country_code=a.country_code AND b.dt=a.dt 
        FULL OUTER JOIN pick_distance_taked AS c ON c.city_id=a.city_id AND c.serv_type=a.serv_type AND c.country_code=a.country_code AND c.dt=a.dt 
        FULL OUTER JOIN dirvers_pushed AS d ON d.city_id=a.city_id AND d.serv_type=a.serv_type AND d.country_code=a.country_code AND d.dt=a.dt 
        FULL OUTER JOIN push_data_chose AS e ON e.city_id=a.city_id AND e.serv_type=a.serv_type AND e.country_code=a.country_code AND e.dt=a.dt
    '''.format(sql=count_city_type_hql, table=hive_table),
    schema='oride_dw',
    dag=dag
)


dependence_dwd_oride_order_push_driver_detail_di >> sleep_time
dependence_dwd_oride_order_assign_driver_detail_di >> sleep_time
dependence_dwd_oride_order_dispatch_chose_detail_di >> sleep_time
dependence_ods_oride_data_order >> sleep_time
dependence_ods_data_driver_extend >> sleep_time
dependence_dwd_oride_driver_accept_order_click_detail_di >> sleep_time
dependence_dwd_oride_driver_accept_order_show_detail_di >> sleep_time

create_result_table_task >> sleep_time

sleep_time >> insert_result_to_hive_all
sleep_time >> insert_result_to_hive_city
sleep_time >> insert_result_to_hive_city_type

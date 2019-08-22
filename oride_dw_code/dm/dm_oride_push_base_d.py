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
    'start_date': datetime(2019, 8, 10),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'dm_oride_push_base_d',
    schedule_interval="00 06 * * *",
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
dependence_ods_oride_server_magic = S3PrefixSensor(
    task_id='dependence_ods_oride_server_magic',
    prefix='oride_buried/ordm.dispatch_tracker_server_magic/dt={pt}'.format(pt='{{ ds }}'),
    bucket_name='opay-bi',
    dag=dag
)

dependence_ods_oride_data_order = UFileSensor(
    task_id='dependence_ods_oride_data_order',
    filepath='{hdfs_path_str}/dt={pt}'.format(
        hdfs_path_str="oride/db/data_order",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dependence_ods_data_driver_extend = UFileSensor(
    task_id='dependence_ods_data_driver_extend',
    filepath='{hdfs_path_str}/dt={pt}'.format(
        hdfs_path_str="oride/db/data_driver_extend",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dependence_ods_oride_client_event = WebHdfsSensor(
    task_id='dependence_ods_oride_client_event',
    filepath='{hdfs_path_str}/dt={pt}/hour=23'.format(
        hdfs_path_str="/user/hive/warehouse/oride_bi.db/oride_client_event_detail",
        pt='{{ ds }}'
    ),
    dag=dag
)
"""
/---------------------------------- end ----------------------------------/
"""

hive_table = 'oride_dw.dm_oride_push_base_d'

create_result_table_task = HiveOperator(
    task_id='create_result_table_task',
    hql='''
        CREATE TABLE IF NOT EXISTS {table} (
            city_id                         bigint          comment '城市ID',
            product_id                      bigint          comment '业务线',
            avg_pushed_distance             decimal(10,2)   comment '平均播单距离',
            avg_picked_distance_done        decimal(10,2)   comment '平均接驾距离(完单)',
            avg_picked_distance_uscancel    decimal(10,2)   comment '平均用户取消订单接驾距离',
            avg_picked_distance_take        decimal(10,2)   comment '平均接驾距离(应答)',
            drivers_pushed_success          bigint          comment '成功推送司机数',
            drivers_count_pushed            bigint          comment '推送给司机的总次数(成功)sum（每个订单被推送的总次数)',
            drivers_count_pushed_all        bigint          comment '推送给司机的总次数(全部)',
            orders_pushed                   bigint          comment '推单数',
            orders_pushed_success           bigint          comment '成功推送订单数',
            drivers_pushed_round            bigint          comment 'SUM(每个订单每轮被推送的司机数去重)'
        ) 
        PARTITIONED BY (
            `country_code` string COMMENT '二位国家码',  
            `dt` string comment '日期'
        )
        STORED AS ORC 
        LOCATION 'ufile://opay-datalake/oride/oride_dw/dm_oride_push_base_d'
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


drop_partitons_from_table = PythonOperator(
    task_id='drop_partitons_from_table',
    python_callable=drop_partions,
    provide_context=True,
    dag=dag
)

# 全部（不分城市，不分业务）
count_hql = '''
--推单距离 
push_distance_finished AS 
(
    SELECT 
        0 AS city_id,
        -1 AS serv_type,
        sm.dt,
        sm.country_code,
        ROUND(IF(SUM(IF(success=1,1,0))>0,
            SUM(IF(success=1,sm.dis,0))/SUM(IF(success=1,1,0)), 
            0), 
        2) AS avg_pushed_distance,                                                      --平均播单距离 
        COUNT(DISTINCT IF(success=1, driver_id, null)) AS drivers_pushed_success,       --成功推送司机数
        SUM(IF(success=1, 1, 0)) AS drivers_count_pushed,                               --推送给司机的总次数(成功)sum（每个订单被推送的总次数)
        COUNT(1) AS drivers_count_pushed_all,                                           --推送给司机的总次数(全部)
        COUNT(DISTINCT order_id) AS orders_pushed，                                     --推单数
        COUNT(DISTINCT IF(success=1, order_id, null)) AS orders_pushed_success          --成功推送订单数
    FROM (SELECT 
            dt, 
            'nal' AS country_code,
            CAST(get_json_object(event_values, '$.success') AS int) AS success,
            CAST(get_json_object(event_values, '$.driver_id') AS bigint) AS driver_id,
            CAST(get_json_object(event_values, '$.distance') AS double) AS dis, 
            get_json_object(event_values, '$.order_id') AS order_id 
        FROM oride_source.dispatch_tracker_server_magic 
        WHERE dt = '{pt}' AND 
            event_name = 'dispatch_push_driver' 
        ) AS sm    
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
            SUM(IF(o.status in (4, 5), m.dis, 0))/SUM(IF(o.status in (4, 5), 1, 0)), 
            0), 
        2) AS avg_picked_distance_done,                                  --平均接驾距离(完单)
        ROUND(IF(SUM(IF(o.status=6 AND o.cancel_role=1, 1, 0))>0, 
            SUM(IF(o.status=6 AND o.cancel_role=1, m.dis, 0))/SUM(IF(o.status=6 AND o.cancel_role=1, 1, 0)), 
            0), 
        2) AS avg_picked_distance_uscancel                              --平均用户取消订单接驾距离
    FROM 
        (SELECT 
            *
        FROM 
            (SELECT 
                dt,
                'nal' AS country_code,
                split(replace(replace(get_json_object(event_values, '$.distances'),'[',''),']',''), ',') AS distances,
                get_json_object(event_values, '$.order_id') AS order_id
            FROM oride_source.dispatch_tracker_server_magic 
            WHERE dt = '{pt}' AND 
                event_name = 'dispatch_assign_driver' 
            ) AS t 
            lateral view posexplode(distances) ds AS dspos, dis 
        ) AS m 
    INNER JOIN (SELECT 
            id, 
            status, 
            cancel_role 
        FROM oride_db.data_order 
        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}'
        ) AS o 
    ON CAST(m.order_id AS bigint) = o.id 
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
            SUM(m.dis)/COUNT(1), 
            0), 
        2) AS avg_picked_distance_take                                 --平均接驾距离(应答)
    FROM 
        (SELECT 
            *
        FROM 
            (SELECT 
                dt,
                'nal' AS country_code,
                split(replace(replace(get_json_object(event_values, '$.distances'),'[',''),']',''), ',') AS distances,
                get_json_object(event_values, '$.order_id') AS order_id
            FROM oride_source.dispatch_tracker_server_magic 
            WHERE dt = '{pt}' AND 
                event_name='dispatch_assign_driver' 
            ) AS t 
            lateral view posexplode(distances) ds AS dspos, dis 
        ) AS m 
    INNER JOIN (SELECT 
            DISTINCT get_json_object(event_value, '$.order_id') AS id 
        FROM oride_bi.oride_client_event_detail 
        WHERE dt = '{pt}' AND 
            event_name = 'accept_order_click'
        ) AS o 
    ON CAST(m.order_id AS bigint) = CAST(o.id AS bigint) 
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
            smround,
            COUNT(DISTINCT driver_id) AS drivers
        FROM (SELECT 
                dt,
                'nal' AS country_code,
                CAST(get_json_object(event_values, '$.driver_id') AS bigint) AS driver_id,
                CAST(get_json_object(event_values, '$.order_id') AS bigint) AS order_id,
                get_json_object(event_values, '$.round') AS smround
            FROM oride_source.dispatch_tracker_server_magic 
            WHERE dt='{pt}' AND 
                event_name = 'dispatch_push_driver' AND  
                get_json_object(event_values, '$.success') = 1
            ) AS sm 
        GROUP BY dt, order_id, smround
        ) AS t 
    GROUP BY country_code, dt
)
'''

insert_result_to_hive_all = HiveOperator(
    task_id='insert_result_to_hive_all',
    hql='''
        SET mapred.job.queue.name=root.users.airflow;
        SET hive.exec.parallel=true;
        SET hive.exec.dynamic.partition=true;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        WITH {sql} 
        INSERT INTO {table} PARTITION (country_code, dt) 
        SELECT 
            NVL(a.city_id, NVL(b.city_id, NVL(c.city_id, d.city_id))),
            NVL(a.serv_type, NVL(b.serv_type, NVL(c.serv_type, d.serv_type))),
            NVL(a.avg_pushed_distance, 0),
            NVL(b.avg_picked_distance_done, 0),
            NVL(b.avg_picked_distance_uscancel, 0),
            NVL(c.avg_picked_distance_take, 0),
            NVL(a.drivers_pushed_success, 0), 
            NVL(a.drivers_count_pushed, 0),
            NVL(a.drivers_count_pushed_all, 0),
            NVL(a.orders_pushed, 0),
            NVL(a.orders_pushed_success, 0),
            NVL(d.drivers_pushed_round, 0), 
            NVL(a.country_code, NVL(b.country_code, NVL(c.country_code, d.country_code))) AS country_code, 
            NVL(a.dt, NVL(b.dt, NVL(c.dt, d.dt))) AS dt 
        FROM push_distance_finished AS a 
        FULL OUTER JOIN pick_distance_finished AS b ON b.city_id=a.city_id AND b.serv_type=a.serv_type AND b.country_code=a.country_code AND b.dt=a.dt 
        FULL OUTER JOIN pick_distance_taked AS c ON c.city_id=a.city_id AND c.serv_type=a.serv_type AND c.country_code=a.country_code AND c.dt=a.dt 
        FULL OUTER JOIN dirvers_pushed AS d ON d.city_id=a.city_id AND d.serv_type=a.serv_type AND d.country_code=a.country_code AND d.dt=a.dt 
    '''.format(sql=count_hql, table=hive_table),
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
        de.country_code,
        de.dt,
        ROUND(IF(SUM(IF(sm.success=1,1,0))>0,
            SUM(IF(sm.success=1,sm.dis,0))/SUM(IF(sm.success=1,1,0)), 
            0), 
        2) AS avg_pushed_distance,                                                          --平均播单距离
        COUNT(DISTINCT IF(sm.success=1, sm.driver_id, null)) AS drivers_pushed_success,     --成功推送司机数
        SUM(IF(sm.success=1, 1, 0)) AS drivers_count_pushed,                                --推送给司机的总次数(成功)sum（每个订单被推送的总次数)
        COUNT(1) AS drivers_count_pushed_all                                                --推送给司机的总次数(全部) 
    FROM (SELECT 
            id,  
            city_id, 
            'nal' AS country_code, 
            dt 
        FROM oride_db.data_driver_extend 
        WHERE dt = '{pt}'
        ) AS de  
    INNER JOIN (SELECT 
            CAST(get_json_object(event_values, '$.success') AS int) AS success,
            CAST(get_json_object(event_values, '$.driver_id') AS bigint) AS driver_id,
            get_json_object(event_values, '$.distance') AS dis 
        FROM oride_source.dispatch_tracker_server_magic 
        WHERE dt = '{pt}' AND 
            event_name = 'dispatch_push_driver' 
        ) AS sm 
    WHERE sm.driver_id = de.id  
    GROUP BY de.country_code, de.dt, de.city_id 
), 
--平均接驾距离(完单), 平均用户取消订单接驾距离
pick_distance_finished AS 
(
    SELECT 
        de.city_id,
        -1 AS serv_type,
        de.country_code,
        de.dt,
        ROUND(IF(SUM(IF(o.status IN (4, 5), 1, 0))>0, 
            SUM(IF(o.status IN (4, 5), m.dis, 0))/SUM(IF(o.status IN (4, 5), 1, 0)), 
            0), 
        2) AS avg_picked_distance_done,                                  --平均接驾距离(完单)
        ROUND(IF(SUM(IF(o.status=6 AND o.cancel_role=1, 1, 0))>0, 
            SUM(IF(o.status=6 AND o.cancel_role=1, m.dis, 0))/SUM(IF(o.status=6 AND o.cancel_role=1, 1, 0)), 
            0), 
        2) AS avg_picked_distance_uscancel                              --平均用户取消订单接驾距离
    FROM 
        (SELECT 
            *
        FROM 
            (SELECT 
                dt,
                split(replace(replace(get_json_object(event_values, '$.driver_ids'),'[',''),']',''), ',') AS drivers, 
                split(replace(replace(get_json_object(event_values, '$.distances'),'[',''),']',''), ',') AS distances,
                get_json_object(event_values, '$.order_id') AS order_id
            FROM oride_source.dispatch_tracker_server_magic 
            WHERE dt = '{pt}' AND 
                event_name = 'dispatch_assign_driver' 
            ) AS t 
            lateral view posexplode(drivers) d AS dpos, driver_id 
            lateral view posexplode(distances) ds AS dspos, dis 
            WHERE dpos = dspos
        ) AS m 
    INNER JOIN (SELECT 
            id, 
            status, 
            cancel_role
        FROM oride_db.data_order 
        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}'
        ) AS o 
    INNER JOIN (SELECT 
            id,  
            city_id, 
            'nal' AS country_code, 
            dt 
        FROM oride_db.data_driver_extend 
        WHERE dt = '{pt}'
        ) AS de 
    WHERE 
        CAST(m.order_id AS bigint) = o.id and 
        CAST(m.driver_id AS bigint) = de.id 
    GROUP BY de.country_code, de.dt, de.city_id 
),
--平均接驾距离(应答)
pick_distance_taked AS 
(
    SELECT 
        de.city_id,
        -1 AS serv_type,
        de.country_code,
        de.dt,
        ROUND(IF(COUNT(1)>0, 
            SUM(m.dis)/COUNT(1), 
            0), 
        2) AS avg_picked_distance_take                                 --平均接驾距离(应答)
    FROM 
        (SELECT 
            *
        FROM 
            (SELECT 
                dt,
                split(replace(replace(get_json_object(event_values, '$.driver_ids'),'[',''), ']',''), ',') as drivers, 
                split(replace(replace(get_json_object(event_values, '$.distances'),'[',''),']',''), ',') as distances,
                get_json_object(event_values, '$.order_id') as order_id
            FROM oride_source.dispatch_tracker_server_magic 
            WHERE dt = '{pt}' AND 
                event_name = 'dispatch_assign_driver' 
            ) AS t 
            lateral view posexplode(drivers) d AS dpos, driver_id 
            lateral view posexplode(distances) ds AS dspos, dis 
            WHERE dpos = dspos
        ) AS m 
    INNER JOIN (SELECT 
            DISTINCT get_json_object(event_value, '$.order_id') AS id 
        FROM oride_bi.oride_client_event_detail 
        WHERE dt = '{pt}' AND 
            event_name = 'accept_order_click'
        ) AS o ON CAST(m.order_id AS bigint) = CAST(o.id AS bigint) 
    INNER JOIN (SELECT 
            id, 
            city_id, 
            'nal' AS country_code, 
            dt 
        FROM oride_db.data_driver_extend 
        WHERE dt = '{pt}'
        ) AS de ON CAST(m.driver_id AS bigint) = de.id  
    GROUP BY de.country_code, de.dt, de.city_id
),

'''


dependence_ods_oride_server_magic >> sleep_time
dependence_ods_oride_data_order >> sleep_time
dependence_ods_data_driver_extend >> sleep_time
dependence_ods_oride_client_event >> sleep_time

create_result_table_task >> drop_partitons_from_table >> sleep_time

sleep_time >> insert_result_to_hive_all

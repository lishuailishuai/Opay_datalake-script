# -*- coding: utf-8 -*-
"""
司机指标模型
"""
import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors import UFileSensor
from airflow.sensors import WebHdfsSensor
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
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
    'app_oride_driver_base_d',
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

dependence_ods_oride_data_driver_records_day = UFileSensor(
    task_id='dependence_ods_oride_data_driver_records_day',
    filepath='{hdfs_path_str}/dt={pt}'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_records_day",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dependence_ods_oride_data_driver_recharge_records = UFileSensor(
    task_id='dependence_ods_oride_data_driver_recharge_records',
    filepath='{hdfs_path_str}/dt={pt}'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_recharge_records",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

dependence_ods_oride_data_driver_reward = UFileSensor(
    task_id='dependence_ods_oride_data_driver_reward',
    filepath='{hdfs_path_str}/dt={pt}'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_reward",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,
    dag=dag
)

# 依赖前一天分区
dependence_oride_driver_timerange = HivePartitionSensor(
    task_id="dependence_oride_driver_timerange",
    table="ods_log_oride_driver_timerange",
    partition="dt='{{ds}}'",
    schema="oride_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
"""
/---------------------------------- end ----------------------------------/
"""

hive_table = 'oride_dw.app_oride_driver_base_d'

create_result_table_task = HiveOperator(
    task_id='create_result_table_task',
    hql='''
        CREATE TABLE IF NOT EXISTS {table} (
            city_id                         bigint          comment '城市ID',
            product_id                      bigint          comment '业务线',
            orders_clicked_all              bigint          comment 'sum(每个司机应答的订单数去重)',
            driver_clicked_count            bigint          comment '司机应答总次数sum(每个司机应答的次数)',
            drivers_clicked                 bigint          comment '应答司机数',
            driver_avg_clicked              decimal(10,2)   comment '人均应答次数', 
            order_showed_count              bigint          comment '订单推送展示次数(推送成功次数)',
            drivers_showed                  bigint          comment '展示司机数(成功推送司机数)',
            orders_showed                   bigint          comment '成功推送总订单数,sum(每个司机被推送的订单数去重)',
            payed_online                    decimal(38,2)   comment '线上支付',
            payed_offline                   decimal(38,2)   comment '下线支付',
            driver_reward                   decimal(38,2)   comment '司机奖励',
            driver_do_range                 bigint          comment '完单司机做单时长(秒)',
            drivers_order_finished          bigint          comment '完单司机数',
            driver_free_range               bigint          comment '司机空闲时长',
            drvers_online                   bigint          comment '在线司机数'
        ) 
        PARTITIONED BY (
            `type` string COMMENT '类型',
            `country_code` string COMMENT '二位国家码',  
            `dt` string comment '日期'
        )
        STORED AS ORC 
        LOCATION 'ufile://opay-datalake/oride/oride_dw/app_oride_driver_base_d'
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

# 不分城市，不分业务
count_hql_all = '''
--sum(每个司机应答的订单数去重)
sum_driver_click_orders AS 
(
    SELECT 
        0 AS city_id,
        -1 AS serv_type,
        country_code, 
        dt,
        SUM(orders) AS orders_clicked_all                                           --sum(每个司机应答的订单数去重)
    FROM 
        (SELECT 
            dt,
            country_code,
            COUNT(DISTINCT order_id) AS orders, 
            driver_id 
        FROM oride_dw.dwd_oride_driver_accept_order_click_detail_di 
        WHERE dt = '{pt}' 
        GROUP BY country_code, dt, driver_id
        ) AS o  
    GROUP BY country_code, dt     
),
--司机应答总次数, 应答司机数, 人均应答次数
click_data AS 
(
    SELECT 
        0 AS city_id,
        -1 AS serv_type,
        country_code, 
        dt,
        COUNT(1) AS driver_clicked_count,                                           --司机应答总次数sum(每个司机应答的次数)
        COUNT(DISTINCT driver_id) AS drivers_clicked,                                 --应答司机数
        ROUND(IF(COUNT(DISTINCT driver_id)>0, 
            COUNT(1)/COUNT(DISTINCT driver_id), 
            0), 
        2) AS driver_avg_clicked                                                    --人均应答次数
    FROM oride_dw.dwd_oride_driver_accept_order_click_detail_di 
    WHERE dt = '{pt}'   
    GROUP BY country_code, dt 
),
--订单show数据
orders_show AS 
(
    SELECT 
        0 AS city_id,
        -1 AS serv_type,
        country_code, 
        dt,
        SUM(show_count) AS order_showed_count,                          --订单推送展示次数(推送成功次数)
        COUNT(DISTINCT t.driver_id) AS drivers_showed,                    --展示司机数（成功推送司机数）
        IF(COUNT(DISTINCT t.driver_id)>0, 
            SUM(show_count)/COUNT(DISTINCT t.driver_id), 
            0
        ) AS driver__avg_show,                                          --人均展示次数
        SUM(order_id) AS orders_showed                                  --成功推送总订单数,sum(每个司机被推送的订单数去重)
    FROM (
        SELECT 
            dt, 
            country_code, 
            COUNT(DISTINCT order_id) AS order_id,
            driver_id,
            COUNT(1) AS show_count
        FROM oride_dw.dwd_oride_driver_accept_order_show_detail_di 
        WHERE dt = '{pt}' 
        GROUP BY country_code, dt, driver_id
        ) AS t 
    GROUP BY country_code, dt
),
--司机收入
income_data AS 
(
    SELECT 
        0 AS city_id,
        -1 AS serv_type,
        drd.country_code, 
        drd.dt,
        SUM(drd.amount_pay_online) AS payed_online,           --线上支付
        SUM(drd.amount_pay_offline) AS payed_offline          --下线支付
    FROM (SELECT 
            amount_pay_online,
            amount_pay_offline,
            driver_id,
            'nal' AS country_code, 
            dt

        FROM oride_dw_ods.ods_sqoop_base_data_driver_records_day_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(day, 'yyyy-MM-dd') = '{pt}'
        ) AS drd 
    JOIN 
        (SELECT 
            driver_id 

        FROM oride_dw_ods.ods_sqoop_base_data_order_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}' AND 
            status IN (4,5)
        GROUP BY driver_id
        ) AS do 
    ON drd.driver_id = do.driver_id 
    GROUP BY drd.country_code, drd.dt 
),
--司机奖励
reward_data AS 
(
    SELECT 
        0 AS city_id,
        -1 AS serv_type,
        do.country_code, 
        do.dt,
        SUM(IF(isnotnull(rr.amount) AND rr.amount>0, rr.amount, 0)) + 
            SUM(IF(isnotnull(dr.amount) AND dr.amount>0, dr.amount, 0)) AS driver_reward       ---司机奖励
    FROM 
        (SELECT 
            dt,
            'nal' AS country_code,
            driver_id 

        FROM oride_dw_ods.ods_sqoop_base_data_order_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}' AND  
            status IN (4,5)
        GROUP BY dt, driver_id 
        ) AS do 
    JOIN (SELECT 
            amount,
            driver_id

        FROM oride_dw_ods.ods_sqoop_base_data_driver_recharge_records_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(created_at, 'yyyy-MM-dd') = '{pt}'
        ) AS rr ON do.driver_id = rr.driver_id 
    JOIN (SELECT 
            amount,
            driver_id  

        FROM oride_dw_ods.ods_sqoop_base_data_driver_reward_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}'
        ) AS dr ON do.driver_id = dr.driver_id 
    GROUP BY do.country_code, do.dt
),
--司机做单时长
driver_range AS 
(
    SELECT 
        0 AS city_id,
        -1 AS serv_type,
        do.country_code, 
        do.dt,
        SUM(IF(do.status=4, 
                ABS(do.arrive_time-do.take_time), 
                IF(do.status=5, 
                    ABS(do.finish_time-do.take_time),
                    ABS(do.cancel_time-do.take_time)
                )
        )) AS driver_do_range,                                                 ---完单司机做单时长
        COUNT(DISTINCT do.driver_id) AS drivers_order_finished                 ---完单司机数
    FROM (SELECT 
            'nal' AS country_code, 
            dt,
            status,
            take_time,
            arrive_time,
            finish_time,
            cancel_time,
            driver_id

        FROM oride_dw_ods.ods_sqoop_base_data_order_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd')='{pt}' AND 
            status IN (4,5,6) 
        ) AS do 
    JOIN (SELECT 
            DISTINCT driver_id 

        FROM oride_dw_ods.ods_sqoop_base_data_order_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd')='{pt}' AND   
            status IN (4,5)
        ) AS off 
    ON do.driver_id = off.driver_id 
    GROUP BY do.country_code, do.dt
),
--司机空闲时长
driver_online AS 
(
    SELECT 
        0 AS city_id,
        -1 AS serv_type,
        odt.country_code, 
        odt.dt,
        SUM(IF(isnull(off.driver_id), 0, odt.driver_freerange)) AS driver_free_range,           --司机空闲时长
        SUM(IF(odt.driver_onlinerange>0, 1, 0)) AS drvers_online                                --在线司机数
    FROM (SELECT
            dt,
            'nal' AS country_code, 
            driver_onlinerange,
            driver_freerange,
            driver_id 
        FROM oride_dw_ods.ods_log_oride_driver_timerange
        WHERE dt = '{pt}'
        ) AS odt 
    JOIN (SELECT 
            distinct driver_id 

        FROM oride_dw_ods.ods_sqoop_base_data_order_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd')='{pt}' AND 
            status IN (4,5)
        ) AS off 
    ON off.driver_id = odt.driver_id 
    GROUP BY odt.country_code, odt.dt
) 
'''.format(pt='{{ ds }}')

insert_result_to_hive_all = HiveOperator(
    task_id='insert_result_to_hive_all',
    hql='''
        SET mapred.job.queue.name=root.users.airflow;
        SET hive.exec.parallel=true;
        SET hive.exec.dynamic.partition=true;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        --SET hive.execution.engine=tez;
        --SET hive.mapjoin.hybridgrace.hashtable=false;
        --set hive.vectorized.execution.enabled=false;
        --SET hive.vectorized.execution.enabled=true;
        --SET hive.vectorized.execution.reduce.enabled=true;
        --SET hive.prewarm.enabled=true;
        --SET hive.prewarm.numcontainers=16;
        --SET hive.tez.auto.reducer.parallelism=true;
        WITH {sql} 
        INSERT OVERWRITE TABLE {table} PARTITION (type, country_code, dt) 
        SELECT 
            NVL(a.city_id, NVL(b.city_id, NVL(c.city_id, NVL(d.city_id, NVL(e.city_id, NVL(f.city_id, g.city_id)))))),
            NVL(a.serv_type, NVL(b.serv_type, NVL(c.serv_type, NVL(d.serv_type, NVL(e.serv_type, NVL(f.serv_type, g.serv_type)))))),
            CASE WHEN a.orders_clicked_all IS NULL THEN 0 ELSE a.orders_clicked_all END,
            CASE WHEN b.driver_clicked_count IS NULL THEN 0 ELSE b.driver_clicked_count END,
            CASE WHEN b.drivers_clicked IS NULL THEN 0 ELSE b.drivers_clicked END,
            CASE WHEN b.driver_avg_clicked IS NULL THEN 0 ELSE b.driver_avg_clicked END,
            CASE WHEN c.order_showed_count IS NULL THEN 0 ELSE c.order_showed_count END, 
            CASE WHEN c.drivers_showed IS NULL THEN 0 ELSE c.drivers_showed END,
            CASE WHEN c.orders_showed IS NULL THEN 0 ELSE c.orders_showed END,
            CASE WHEN d.payed_online IS NULL THEN 0 ELSE d.payed_online END,
            CASE WHEN d.payed_offline IS NULL THEN 0 ELSE d.payed_offline END,
            CASE WHEN e.driver_reward IS NULL THEN 0 ELSE e.driver_reward END,
            CASE WHEN f.driver_do_range IS NULL THEN 0 ELSE f.driver_do_range END,
            CASE WHEN f.drivers_order_finished IS NULL THEN 0 ELSE f.drivers_order_finished END,
            CASE WHEN g.driver_free_range IS NULL THEN 0 ELSE g.driver_free_range END,
            CASE WHEN g.drvers_online IS NULL THEN 0 ELSE g.drvers_online END,
            'all' AS type,
            NVL(a.country_code, NVL(b.country_code, NVL(c.country_code, NVL(d.country_code, NVL(e.country_code, NVL(f.country_code, g.country_code)))))) AS country_code, 
            NVL(a.dt, NVL(b.dt, NVL(c.dt, NVL(d.dt, NVL(e.dt, NVL(f.dt, g.dt)))))) AS dt 
        FROM sum_driver_click_orders AS a 
        FULL OUTER JOIN click_data AS b ON b.city_id=a.city_id AND b.serv_type=a.serv_type AND b.country_code=a.country_code AND b.dt=a.dt 
        FULL OUTER JOIN orders_show AS c ON c.city_id=a.city_id AND c.serv_type=a.serv_type AND c.country_code=a.country_code AND c.dt=a.dt 
        FULL OUTER JOIN income_data AS d ON d.city_id=a.city_id AND d.serv_type=a.serv_type AND d.country_code=a.country_code AND d.dt=a.dt 
        FULL OUTER JOIN reward_data AS e ON e.city_id=a.city_id AND e.serv_type=a.serv_type AND e.country_code=a.country_code AND e.dt=a.dt
        FULL OUTER JOIN driver_range AS f ON f.city_id=a.city_id AND f.serv_type=a.serv_type AND f.country_code=a.country_code AND f.dt=a.dt
        FULL OUTER JOIN driver_online AS g ON g.city_id=a.city_id AND g.serv_type=a.serv_type AND g.country_code=a.country_code AND g.dt=a.dt
    '''.format(sql=count_hql_all, table=hive_table),
    schema='oride_dw',
    dag=dag
)

# 分城市， 不分业务
count_hql_city = '''
--sum(每个司机应答的订单数去重)
sum_driver_click_orders AS 
(
    SELECT 
        de.city_id,
        -1 AS serv_type,
        o.country_code, 
        o.dt,
        SUM(orders) AS orders_clicked_all                   --sum(每个司机应答的订单数去重)
    FROM 
        (SELECT 
            dt, 
            country_code, 
            COUNT(DISTINCT order_id) AS orders, 
            driver_id 
        FROM oride_dw.dwd_oride_driver_accept_order_click_detail_di 
        WHERE dt = '{pt}' 
        GROUP BY country_code, dt, driver_id
        ) AS o  
    JOIN (SELECT 
            id, 
            city_id 

        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 

        WHERE dt = '{pt}'
        ) AS de 
    ON o.driver_id = de.id  
    GROUP BY o.country_code, o.dt, de.city_id    
),
--司机应答总次数, 应答司机数, 人均应答次数
click_data AS 
(
    SELECT 
        do.city_id,
        -1 AS serv_type,
        ce.country_code, 
        ce.dt,
        COUNT(1) AS driver_clicked_count,                                        --司机应答总次数sum(每个司机应答的次数)
        COUNT(DISTINCT ce.driver_id) AS drivers_clicked,                           --应答司机数
        ROUND(IF(COUNT(DISTINCT ce.driver_id)>0, 
            COUNT(1)/COUNT(DISTINCT ce.driver_id), 
            0), 
        2) AS driver_avg_clicked                                                  --人均应答次数
    FROM
        (SELECT 
            dt, 
            country_code, 
            driver_id
        FROM oride_dw.dwd_oride_driver_accept_order_click_detail_di 
        WHERE dt = '{pt}' 
        ) AS ce 
    JOIN (SELECT 
            id, 
            city_id 

        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 

        WHERE dt = '{pt}'
        ) AS do  
    WHERE ce.driver_id = do.id  
    GROUP BY ce.country_code, ce.dt, do.city_id
),
--订单show数据
orders_show AS 
(
    SELECT 
        do.city_id,
        -1 AS serv_type,
        t.country_code, 
        t.dt,
        SUM(t.show_count) AS order_showed_count,                            --订单推送展示次数(推送成功次数)
        COUNT(DISTINCT t.driver_id) AS drivers_showed,                        --展示司机数（成功推送司机数）
        ROUND(IF(COUNT(DISTINCT t.driver_id)>0, 
            SUM(show_count)/COUNT(DISTINCT t.driver_id), 
            0), 
        2) AS driver_avg_show,                                              --人均展示次数
        SUM(t.order_id) AS orders_showed                                    --成功推送总订单数,sum(每个司机被推送的订单数去重)
    FROM (
        SELECT 
            dt, 
            country_code, 
            COUNT(DISTINCT order_id) AS order_id,
            driver_id,
            COUNT(1) AS show_count
        FROM oride_dw.dwd_oride_driver_accept_order_show_detail_di 
        WHERE dt = '{pt}' 
        GROUP BY country_code, dt, driver_id
        ) AS t 
    JOIN (SELECT 
            id, 
            city_id 

        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 

        WHERE dt = '{pt}'
        ) AS do  
    WHERE t.driver_id = do.id  
    GROUP BY t.country_code, t.dt, do.city_id
),
--司机收入
income_data AS 
(
    SELECT 
        de.city_id,
        -1 AS serv_type,
        de.country_code, 
        de.dt,
        SUM(drd.amount_pay_online) AS payed_online,           --线上支付
        SUM(drd.amount_pay_offline) AS payed_offline          --下线支付
    FROM 
        (SELECT 
            driver_id 

        FROM oride_dw_ods.ods_sqoop_base_data_order_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}' AND 
            status IN (4,5)
        GROUP BY driver_id
        ) AS do 
    JOIN (SELECT 
            id, 
            city_id, 
            'nal' AS country_code, 
            dt 

        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 

        WHERE dt = '{pt}'
        ) AS de ON de.id = do.driver_id 
    JOIN (SELECT 
            amount_pay_online,
            amount_pay_offline,
            driver_id,
            dt

        FROM oride_dw_ods.ods_sqoop_base_data_driver_records_day_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(day, 'yyyy-MM-dd') = '{pt}'
        ) AS drd ON drd.driver_id = do.driver_id  
    GROUP BY de.country_code, de.dt, de.city_id 
),
--司机奖励
reward_data AS 
(
    SELECT 
        de.city_id,
        -1 AS serv_type,
        de.country_code, 
        de.dt,
        SUM(IF(isnotnull(rr.amount) AND rr.amount>0, rr.amount, 0)) + 
            SUM(IF(isnotnull(dr.amount) AND dr.amount>0, dr.amount, 0)) AS driver_reward        ---司机奖励
    FROM 
        (SELECT 
            driver_id 

        FROM oride_dw_ods.ods_sqoop_base_data_order_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}' AND  
            status IN (4,5)
        GROUP BY driver_id 
        ) AS do 
    JOIN (SELECT 
            id, 
            city_id, 
            'nal' AS country_code, 
            dt 

        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 

        WHERE dt = '{pt}'
        ) AS de ON de.id = do.driver_id 
    JOIN (SELECT 
            amount,
            driver_id

        FROM oride_dw_ods.ods_sqoop_base_data_driver_recharge_records_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(created_at, 'yyyy-MM-dd') = '{pt}'
        ) AS rr ON do.driver_id = rr.driver_id 
    JOIN (SELECT 
            amount,
            driver_id  

        FROM oride_dw_ods.ods_sqoop_base_data_driver_reward_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}'
        ) AS dr ON do.driver_id = dr.driver_id 
    GROUP BY de.country_code, de.dt, de.city_id
),
--司机做单时长
driver_range AS 
(
    SELECT 
        de.city_id,
        -1 AS serv_type,
        de.country_code, 
        de.dt,
        SUM(IF(do.status=4, 
                ABS(do.arrive_time-do.take_time), 
                IF(do.status=5, 
                    ABS(do.finish_time-do.take_time),
                    ABS(do.cancel_time-do.take_time)
                )
        )) AS driver_do_range,                                                  ---完单司机做单时长
        COUNT(DISTINCT do.driver_id) AS drivers_order_finished                  ---完单司机数
    FROM (SELECT 
            status,
            take_time,
            arrive_time,
            finish_time,
            cancel_time,
            driver_id

        FROM oride_dw_ods.ods_sqoop_base_data_order_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd')='{pt}' AND 
            status IN (4,5,6) 
        ) AS do 
    JOIN (SELECT 
            DISTINCT driver_id 

        FROM oride_dw_ods.ods_sqoop_base_data_order_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd')='{pt}' AND   
            status IN (4,5)
        ) AS off ON do.driver_id = off.driver_id 
    JOIN (SELECT 
            id, 
            city_id, 
            'nal' AS country_code, 
            dt 

        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 

        WHERE dt = '{pt}'
        ) AS de ON de.id = do.driver_id 
    GROUP BY de.country_code, de.dt, de.city_id
),
--司机空闲时长
driver_online AS 
(
    SELECT 
        de.city_id,
        -1 AS serv_type,
        de.country_code, 
        de.dt,
        SUM(IF(isnull(off.driver_id), 0, odt.driver_freerange)) AS driver_free_range,           --司机空闲时长
        SUM(IF(odt.driver_onlinerange>0, 1, 0)) AS drvers_online                                --在线司机数
    FROM (SELECT 
            driver_onlinerange,
            driver_freerange,
            driver_id 
        FROM oride_dw_ods.ods_log_oride_driver_timerange
        WHERE dt = '{pt}'
        ) AS odt 
    JOIN (SELECT 
            distinct driver_id 

        FROM oride_dw_ods.ods_sqoop_base_data_order_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd')='{pt}' AND 
            status IN (4,5)
        ) AS off ON off.driver_id = odt.driver_id 
    JOIN (SELECT 
            id, 
            city_id, 
            'nal' AS country_code, 
            dt 

        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 

        WHERE dt = '{pt}'
        ) AS de ON odt.driver_id = de.id 
    GROUP BY de.country_code, de.dt, de.city_id
) 
'''.format(pt='{{ ds }}')

insert_result_to_hive_city = HiveOperator(
    task_id='insert_result_to_hive_city',
    hql='''
        SET mapred.job.queue.name=root.users.airflow;
        SET hive.exec.parallel=true;
        SET hive.exec.dynamic.partition=true;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        --SET hive.execution.engine=tez;
        --SET hive.mapjoin.hybridgrace.hashtable=false;
        --set hive.vectorized.execution.enabled=false;
        --SET hive.vectorized.execution.enabled=true;
        --SET hive.vectorized.execution.reduce.enabled=true;
        --SET hive.prewarm.enabled=true;
        --SET hive.prewarm.numcontainers=16;
        --SET hive.tez.auto.reducer.parallelism=true;
        WITH {sql} 
        INSERT OVERWRITE TABLE {table} PARTITION (type, country_code, dt) 
        SELECT 
            NVL(a.city_id, NVL(b.city_id, NVL(c.city_id, NVL(d.city_id, NVL(e.city_id, NVL(f.city_id, g.city_id)))))),
            NVL(a.serv_type, NVL(b.serv_type, NVL(c.serv_type, NVL(d.serv_type, NVL(e.serv_type, NVL(f.serv_type, g.serv_type)))))),
            CASE WHEN a.orders_clicked_all IS NULL THEN 0 ELSE a.orders_clicked_all END,
            CASE WHEN b.driver_clicked_count IS NULL THEN 0 ELSE b.driver_clicked_count END,
            CASE WHEN b.drivers_clicked IS NULL THEN 0 ELSE b.drivers_clicked END,
            CASE WHEN b.driver_avg_clicked IS NULL THEN 0 ELSE b.driver_avg_clicked END,
            CASE WHEN c.order_showed_count IS NULL THEN 0 ELSE c.order_showed_count END, 
            CASE WHEN c.drivers_showed IS NULL THEN 0 ELSE c.drivers_showed END,
            CASE WHEN c.orders_showed IS NULL THEN 0 ELSE c.orders_showed END,
            CASE WHEN d.payed_online IS NULL THEN 0 ELSE d.payed_online END,
            CASE WHEN d.payed_offline IS NULL THEN 0 ELSE d.payed_offline END,
            CASE WHEN e.driver_reward IS NULL THEN 0 ELSE e.driver_reward END,
            CASE WHEN f.driver_do_range IS NULL THEN 0 ELSE f.driver_do_range END,
            CASE WHEN f.drivers_order_finished IS NULL THEN 0 ELSE f.drivers_order_finished END,
            CASE WHEN g.driver_free_range IS NULL THEN 0 ELSE g.driver_free_range END,
            CASE WHEN g.drvers_online IS NULL THEN 0 ELSE g.drvers_online END,
            'city' AS type,
            NVL(a.country_code, NVL(b.country_code, NVL(c.country_code, NVL(d.country_code, NVL(e.country_code, NVL(f.country_code, g.country_code)))))) AS country_code, 
            NVL(a.dt, NVL(b.dt, NVL(c.dt, NVL(d.dt, NVL(e.dt, NVL(f.dt, g.dt)))))) AS dt 
        FROM sum_driver_click_orders AS a 
        FULL OUTER JOIN click_data AS b ON b.city_id=a.city_id AND b.serv_type=a.serv_type AND b.country_code=a.country_code AND b.dt=a.dt 
        FULL OUTER JOIN orders_show AS c ON c.city_id=a.city_id AND c.serv_type=a.serv_type AND c.country_code=a.country_code AND c.dt=a.dt 
        FULL OUTER JOIN income_data AS d ON d.city_id=a.city_id AND d.serv_type=a.serv_type AND d.country_code=a.country_code AND d.dt=a.dt 
        FULL OUTER JOIN reward_data AS e ON e.city_id=a.city_id AND e.serv_type=a.serv_type AND e.country_code=a.country_code AND e.dt=a.dt
        FULL OUTER JOIN driver_range AS f ON f.city_id=a.city_id AND f.serv_type=a.serv_type AND f.country_code=a.country_code AND f.dt=a.dt
        FULL OUTER JOIN driver_online AS g ON g.city_id=a.city_id AND g.serv_type=a.serv_type AND g.country_code=a.country_code AND g.dt=a.dt
    '''.format(sql=count_hql_city, table=hive_table),
    schema='oride_dw',
    dag=dag
)

# 分城市， 分业务
count_hql_city_type = '''
--sum(每个司机应答的订单数去重)
sum_driver_click_orders AS 
(
    SELECT 
        de.city_id,
        de.serv_type,
        o.country_code, 
        o.dt,
        SUM(orders) AS orders_clicked_all                   --sum(每个司机应答的订单数去重)
    FROM 
        (SELECT 
            dt, 
            country_code, 
            COUNT(DISTINCT order_id) AS orders, 
            driver_id 
        FROM oride_dw.dwd_oride_driver_accept_order_click_detail_di 
        WHERE dt = '{pt}' 
        GROUP BY country_code, dt, driver_id 
        ) AS o  
    JOIN (SELECT 
            id, 
            city_id, 
            serv_type 

        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 

        WHERE dt = '{pt}'
        ) AS de 
    ON o.driver_id = de.id  
    GROUP BY o.country_code, o.dt, de.city_id, de.serv_type
),
--司机应答总次数, 应答司机数, 人均应答次数
click_data AS 
(
    SELECT 
        do.city_id,
        do.serv_type,
        ce.country_code, 
        ce.dt,
        COUNT(1) AS driver_clicked_count,                                        --司机应答总次数sum(每个司机应答的次数)
        COUNT(DISTINCT ce.driver_id) AS drivers_clicked,                           --应答司机数
        ROUND(IF(COUNT(DISTINCT ce.driver_id)>0, 
            COUNT(1)/COUNT(DISTINCT ce.driver_id), 
            0), 
        2) AS driver_avg_clicked                                                  --人均应答次数
    FROM
        (SELECT 
            dt,
            country_code, 
            driver_id
        FROM oride_dw.dwd_oride_driver_accept_order_click_detail_di 
        WHERE dt = '{pt}' 
        ) AS ce 
    JOIN (SELECT 
            id, 
            city_id,
            serv_type 

        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 

        WHERE dt = '{pt}'
        ) AS do  
    WHERE ce.driver_id = do.id  
    GROUP BY ce.country_code, ce.dt, do.city_id, do.serv_type
),
--订单show数据
orders_show AS 
(
    SELECT 
        do.city_id,
        do.serv_type,
        t.country_code, 
        t.dt,
        SUM(t.show_count) AS order_showed_count,                            --订单推送展示次数(推送成功次数)
        COUNT(DISTINCT t.driver_id) AS drivers_showed,                        --展示司机数（成功推送司机数）
        ROUND(IF(COUNT(DISTINCT t.driver_id)>0, 
            SUM(show_count)/COUNT(DISTINCT t.driver_id), 
            0), 
        2) AS driver_avg_show,                                              --人均展示次数
        SUM(t.order_id) AS orders_showed                                    --成功推送总订单数,sum(每个司机被推送的订单数去重)
    FROM (
        SELECT 
            dt, 
            country_code, 
            COUNT(DISTINCT order_id) AS order_id,
            driver_id,
            COUNT(1) AS show_count
        FROM oride_dw.dwd_oride_driver_accept_order_show_detail_di 
        WHERE dt = '{pt}' 
        GROUP BY country_code, dt, driver_id
        ) AS t 
    JOIN (SELECT 
            id, 
            city_id, 
            serv_type  

        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 

        WHERE dt = '{pt}'
        ) AS do  
    WHERE t.driver_id = do.id  
    GROUP BY t.country_code, t.dt, do.city_id, do.serv_type
),
--司机收入
income_data AS 
(
    SELECT 
        de.city_id,
        de.serv_type,
        de.country_code, 
        de.dt,
        SUM(drd.amount_pay_online) AS payed_online,           --线上支付
        SUM(drd.amount_pay_offline) AS payed_offline          --下线支付
    FROM 
        (SELECT 
            driver_id 

        FROM oride_dw_ods.ods_sqoop_base_data_order_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}' AND 
            status IN (4,5)
        GROUP BY driver_id
        ) AS do 
    JOIN (SELECT 
            id, 
            city_id,
            serv_type, 
            'nal' AS country_code, 
            dt 

        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 

        WHERE dt = '{pt}'
        ) AS de ON de.id = do.driver_id 
    JOIN (SELECT 
            amount_pay_online,
            amount_pay_offline,
            driver_id,
            dt

        FROM oride_dw_ods.ods_sqoop_base_data_driver_records_day_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(day, 'yyyy-MM-dd') = '{pt}'
        ) AS drd ON drd.driver_id = do.driver_id  
    GROUP BY de.country_code, de.dt, de.city_id, de.serv_type 
),
--司机奖励
reward_data AS 
(
    SELECT 
        de.city_id,
        de.serv_type,
        de.country_code, 
        de.dt,
        SUM(IF(isnotnull(rr.amount) AND rr.amount>0, rr.amount, 0)) + 
            SUM(IF(isnotnull(dr.amount) AND dr.amount>0, dr.amount, 0)) AS driver_reward        ---司机奖励
    FROM 
        (SELECT 
            driver_id 

        FROM oride_dw_ods.ods_sqoop_base_data_order_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}' AND  
            status IN (4,5)
        GROUP BY driver_id 
        ) AS do 
    JOIN (SELECT 
            id, 
            city_id, 
            serv_type,
            'nal' AS country_code, 
            dt 

        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 

        WHERE dt = '{pt}'
        ) AS de ON de.id = do.driver_id 
    JOIN (SELECT 
            amount,
            driver_id

        FROM oride_dw_ods.ods_sqoop_base_data_driver_recharge_records_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(created_at, 'yyyy-MM-dd') = '{pt}'
        ) AS rr ON do.driver_id = rr.driver_id 
    JOIN (SELECT 
            amount,
            driver_id  

        FROM oride_dw_ods.ods_sqoop_base_data_driver_reward_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}'
        ) AS dr ON do.driver_id = dr.driver_id 
    GROUP BY de.country_code, de.dt, de.city_id, de.serv_type
),
--司机做单时长
driver_range AS 
(
    SELECT 
        de.city_id,
        de.serv_type,
        de.country_code, 
        de.dt,
        SUM(IF(do.status=4, 
                ABS(do.arrive_time-do.take_time), 
                IF(do.status=5, 
                    ABS(do.finish_time-do.take_time),
                    ABS(do.cancel_time-do.take_time)
                )
        )) AS driver_do_range,                                                  ---完单司机做单时长
        COUNT(DISTINCT do.driver_id) AS drivers_order_finished                  ---完单司机数
    FROM (SELECT 
            status,
            take_time,
            arrive_time,
            finish_time,
            cancel_time,
            driver_id

        FROM oride_dw_ods.ods_sqoop_base_data_order_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd')='{pt}' AND 
            status IN (4,5,6) 
        ) AS do 
    JOIN (SELECT 
            DISTINCT driver_id 

        FROM oride_dw_ods.ods_sqoop_base_data_order_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd')='{pt}' AND   
            status IN (4,5)
        ) AS off ON do.driver_id = off.driver_id 
    JOIN (SELECT 
            id, 
            city_id, 
            serv_type,
            'nal' AS country_code, 
            dt 
        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 

        WHERE dt = '{pt}'
        ) AS de ON de.id = do.driver_id 
    GROUP BY de.country_code, de.dt, de.city_id, de.serv_type
),
--司机空闲时长
driver_online AS 
(
    SELECT 
        de.city_id,
        de.serv_type,
        de.country_code, 
        de.dt,
        SUM(IF(isnull(off.driver_id), 0, odt.driver_freerange)) AS driver_free_range,           --司机空闲时长
        SUM(IF(odt.driver_onlinerange>0, 1, 0)) AS drvers_online                                --在线司机数
    FROM (SELECT 
            driver_onlinerange,
            driver_freerange,
            driver_id 
        FROM oride_dw_ods.ods_log_oride_driver_timerange
        WHERE dt = '{pt}'
        ) AS odt 
    JOIN (SELECT 
            distinct driver_id 

        FROM oride_dw_ods.ods_sqoop_base_data_order_df 

        WHERE dt = '{pt}' AND 
            from_unixtime(create_time, 'yyyy-MM-dd')='{pt}' AND 
            status IN (4,5)
        ) AS off ON off.driver_id = odt.driver_id 
    JOIN (SELECT 
            id, 
            city_id,
            serv_type, 
            'nal' AS country_code, 
            dt 

        FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df 

        WHERE dt = '{pt}'
        ) AS de ON odt.driver_id = de.id 
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
        --SET hive.execution.engine=tez;
        --SET hive.mapjoin.hybridgrace.hashtable=false;
        --set hive.vectorized.execution.enabled=false;
        --SET hive.vectorized.execution.enabled=true;
        --SET hive.vectorized.execution.reduce.enabled=true;
        --SET hive.prewarm.enabled=true;
        --SET hive.prewarm.numcontainers=16;
        --SET hive.tez.auto.reducer.parallelism=true;
        WITH {sql} 
        INSERT OVERWRITE TABLE {table} PARTITION (type, country_code, dt) 
        SELECT 
            NVL(a.city_id, NVL(b.city_id, NVL(c.city_id, NVL(d.city_id, NVL(e.city_id, NVL(f.city_id, g.city_id)))))),
            NVL(a.serv_type, NVL(b.serv_type, NVL(c.serv_type, NVL(d.serv_type, NVL(e.serv_type, NVL(f.serv_type, g.serv_type)))))),
            CASE WHEN a.orders_clicked_all IS NULL THEN 0 ELSE a.orders_clicked_all END,
            CASE WHEN b.driver_clicked_count IS NULL THEN 0 ELSE b.driver_clicked_count END,
            CASE WHEN b.drivers_clicked IS NULL THEN 0 ELSE b.drivers_clicked END,
            CASE WHEN b.driver_avg_clicked IS NULL THEN 0 ELSE b.driver_avg_clicked END,
            CASE WHEN c.order_showed_count IS NULL THEN 0 ELSE c.order_showed_count END, 
            CASE WHEN c.drivers_showed IS NULL THEN 0 ELSE c.drivers_showed END,
            CASE WHEN c.orders_showed IS NULL THEN 0 ELSE c.orders_showed END,
            CASE WHEN d.payed_online IS NULL THEN 0 ELSE d.payed_online END,
            CASE WHEN d.payed_offline IS NULL THEN 0 ELSE d.payed_offline END,
            CASE WHEN e.driver_reward IS NULL THEN 0 ELSE e.driver_reward END,
            CASE WHEN f.driver_do_range IS NULL THEN 0 ELSE f.driver_do_range END,
            CASE WHEN f.drivers_order_finished IS NULL THEN 0 ELSE f.drivers_order_finished END,
            CASE WHEN g.driver_free_range IS NULL THEN 0 ELSE g.driver_free_range END,
            CASE WHEN g.drvers_online IS NULL THEN 0 ELSE g.drvers_online END,
            'serv' AS type,
            NVL(a.country_code, NVL(b.country_code, NVL(c.country_code, NVL(d.country_code, NVL(e.country_code, NVL(f.country_code, g.country_code)))))) AS country_code, 
            NVL(a.dt, NVL(b.dt, NVL(c.dt, NVL(d.dt, NVL(e.dt, NVL(f.dt, g.dt)))))) AS dt 
        FROM sum_driver_click_orders AS a 
        FULL OUTER JOIN click_data AS b ON b.city_id=a.city_id AND b.serv_type=a.serv_type AND b.country_code=a.country_code AND b.dt=a.dt 
        FULL OUTER JOIN orders_show AS c ON c.city_id=a.city_id AND c.serv_type=a.serv_type AND c.country_code=a.country_code AND c.dt=a.dt 
        FULL OUTER JOIN income_data AS d ON d.city_id=a.city_id AND d.serv_type=a.serv_type AND d.country_code=a.country_code AND d.dt=a.dt 
        FULL OUTER JOIN reward_data AS e ON e.city_id=a.city_id AND e.serv_type=a.serv_type AND e.country_code=a.country_code AND e.dt=a.dt
        FULL OUTER JOIN driver_range AS f ON f.city_id=a.city_id AND f.serv_type=a.serv_type AND f.country_code=a.country_code AND f.dt=a.dt
        FULL OUTER JOIN driver_online AS g ON g.city_id=a.city_id AND g.serv_type=a.serv_type AND g.country_code=a.country_code AND g.dt=a.dt
    '''.format(sql=count_hql_city_type, table=hive_table),
    schema='oride_dw',
    dag=dag
)


dependence_dwd_oride_driver_accept_order_click_detail_di >> sleep_time
dependence_dwd_oride_driver_accept_order_show_detail_di >> sleep_time
dependence_ods_data_driver_extend >> sleep_time
dependence_ods_oride_data_order >> sleep_time
dependence_ods_oride_data_driver_records_day >> sleep_time
dependence_ods_oride_data_driver_recharge_records >> sleep_time
dependence_ods_oride_data_driver_reward >> sleep_time
dependence_oride_driver_timerange >> sleep_time

create_result_table_task >> sleep_time

sleep_time >> insert_result_to_hive_all
sleep_time >> insert_result_to_hive_city
sleep_time >> insert_result_to_hive_city_type

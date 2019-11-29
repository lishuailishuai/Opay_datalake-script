# -*- coding: utf-8 -*-
"""
查询全局报表部分数据
"""
import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from datetime import datetime, timedelta


args = {
    'owner': 'wuduo',
    'start_date': datetime(2019, 10, 9),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_oride_city_global_d',
    schedule_interval="30 02 * * *",
    default_args=args
)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 20',
    dag=dag
)

# 依赖
# dependence_table_oride_order_city_daily_report_task = HivePartitionSensor(
#     task_id="dependence_table_oride_order_city_daily_report_task",
#     table="oride_order_city_daily_report",
#     partition="dt='{{ ds }}'",
#     schema="oride_bi",
#     poke_interval=120,
#     dag=dag
# )

dependence_table_ods_sqoop_base_data_city_conf_df_task = HivePartitionSensor(
    task_id="dependence_table_ods_sqoop_base_data_city_conf_df_task",
    table="ods_sqoop_base_data_city_conf_df",
    partition="dt='{{ ds }}'",
    schema="oride_dw_ods",
    poke_interval=120,
    dag=dag
)
# end

hive_table = "oride_dw.app_oride_city_global_d"

count_sql = """
all_data as (
    SELECT
        dt,
        SUM(rain_order_num) AS rain_order_num,
        SUM(request_num) AS request_num,
        SUM(request_num_lfw) AS request_num_lfw,
        SUM(effective_order_num) AS effective_order_num,
        SUM(pay_num) AS pay_num,
        SUM(completed_num) AS completed_num,
        SUM(completed_num_lfw) AS completed_num_lfw,
        SUM(active_users) AS active_users,
        SUM(request_usernum) AS request_usernum,
        SUM(completed_users) AS completed_users,
        SUM(online_pay_user_num) AS online_pay_user_num
    FROM
        oride_bi.oride_order_city_daily_report
    WHERE
        dt='{ds}'
    GROUP BY
        dt
),
city_data as (
    SELECT 
        * 
    FROM (SELECT
            *
        FROM oride_bi.oride_order_city_daily_report
        WHERE dt='{ds}'
        ) AS a 
    LEFT JOIN (SELECT 
            SUM(completed_num) AS completed_num_total 
        FROM oride_bi.oride_order_city_daily_report 
        WHERE dt = '{ds}'
        ) AS b 
    ON 1 = 1
)
""".format(ds='{{ ds }}')

insert_result_to_hive = HiveOperator(
    task_id='insert_result_to_hive',
    hql='''
        SET mapred.job.queue.name=root.users.airflow;
        SET hive.exec.parallel=true;
        SET hive.exec.dynamic.partition=true;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        WITH {sql} 
        INSERT OVERWRITE TABLE {table} PARTITION (dt) 
        -- 全部城市数据
        SELECT
            0 as city_id,                                                                       --城市ID
            'All' as name,                                                                      --城市名
            '-',                                                                                --天气
            100.00,                                                                             --城市完单占比
            nvl(round(rain_order_num/request_num*100, 2), 0),                                   --湿单占比
            request_num,                                                                        --下单数
            effective_order_num,                                                                --有效下单数
            pay_num,                                                                            --支付完单数
            completed_num,                                                                      --完单数
            nvl(round(completed_num/request_num*100, 2), 0),                                    --完单率
            active_users,                                                                       --活跃乘客数
            request_usernum,                                                                    --下单用户数
            completed_users,                                                                    --完单乘客数
            nvl(round(online_pay_user_num/completed_users*100, 2), 0),                          --线上支付乘客占比
            0,                                                                                  --排名
            from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'yyyyMMdd'),                         --数据日期
            unix_timestamp(),                                                                   --更新时间
            dt                                                                                  --分区日期
        FROM
            all_data
        -- 分城市数据
        UNION
        SELECT
            td.id as city_id,                                                                   --城市ID
            td.name as name,                                                                    --城市名
            cd.weather,                                                                         --天气
            nvl(round(cd.completed_num/cd.completed_num_total*100, 2), 0),                      --城市完单占比
            nvl(round(cd.rain_order_num/cd.request_num*100, 2), 0),                             --湿单占比
            cd.request_num,                                                                     --下单数
            cd.effective_order_num,                                                             --有效下单数
            cd.pay_num,                                                                         --支付完单数
            cd.completed_num,                                                                   --完单数
            nvl(round(cd.completed_num/cd.request_num*100, 2), 0),                              --完单率
            cd.active_users,                                                                    --活跃乘客数
            cd.request_usernum,                                                                 --下单用户数
            cd.completed_users,                                                                 --完单乘客数
            nvl(round(cd.online_pay_user_num/cd.completed_users*100, 2), 0),                    --线上支付乘客占比
            row_number() over(partition by cd.dt order by cd.request_num desc),                 --排名
            from_unixtime(unix_timestamp(cd.dt, 'yyyy-MM-dd'),'yyyyMMdd'),                      --数据日期
            unix_timestamp(),                                                                   --更新时间
            cd.dt                                                                               --分区日期
        FROM
            city_data AS cd
            INNER JOIN
            (
                SELECT
                    *
                FROM
                    oride_dw_ods.ods_sqoop_base_data_city_conf_df
                WHERE
                   dt='{ds}'
            ) AS td 
        ON lower(cd.city) = lower(td.name)
    '''.format(sql=count_sql, table=hive_table, ds='{{ ds }}'),
    schema='oride_bi',
    dag=dag
)



dependence_table_ods_sqoop_base_data_city_conf_df_task >> sleep_time

sleep_time >> insert_result_to_hive

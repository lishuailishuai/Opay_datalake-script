import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator

args = {
    'owner': 'root',
    'start_date': datetime(2019, 6, 17),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    'oride_transportation_capacity_control',
    schedule_interval="20 * * * *",
    default_args=args)

add_partitions = HiveOperator(
    task_id='add_partitions',
    hql="""
            ALTER TABLE user_order ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
        """,
    schema='oride_source',
    dag=dag)

create_oride_transportation_capacity_control = HiveOperator(
    task_id='create_oride_transportation_capacity_control',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_transportation_capacity_control (
          dt string,
          hour string,
          h3_id string,
          today_order_num int,
          today_finished_num int,
          order_num int,
          finished_num int,
          no_take_order_num int,
          take_order_num int,
          cancel_after_take_num int
        )
        STORED AS PARQUET
        """,
    schema='dashboard',
    dag=dag)

insert_oride_transportation_capacity_control = HiveOperator(
    task_id='insert_oride_transportation_capacity_control',
    hql="""
        create temporary function geoToH3Address as 'com.oride.udf.GeoToH3Address';
        -- 删除数据
        INSERT OVERWRITE TABLE oride_transportation_capacity_control
        SELECT
            *
        FROM
           oride_transportation_capacity_control
        WHERE
            dt != '{{ ds }}' AND hour !='{{ execution_date.strftime("%H") }}';
        -- 插入数据
        with order_data as (
            select
                dt,
                order_id,
                MAX(struct(`timestamp`, start_location['lat'], start_location['lng'])).col2 AS lat,
                MAX(struct(`timestamp`, start_location['lat'], start_location['lng'])).col3 AS lng,
                MAX(struct(`timestamp`, status)).col2 AS status,
                MAX(struct(`timestamp`, driver_id)).col2 AS driver_id,
                MAX(struct(`timestamp`, user_id)).col2 AS user_id,
                MIN(`timestamp`) AS create_time
            from
                oride_source.user_order
            where
                dt='{{ ds }}' AND hour<='{{ execution_date.strftime("%H") }}'
            group by
                order_id,dt
        )
        INSERT INTO TABLE oride_transportation_capacity_control
        SELECT
            td.dt,
            '{{ execution_date.strftime("%H") }}',
            td.h3_id,
            td.today_order_num,
            td.today_finished_order_num,
            hd.order_num,
            hd.finished_order_num,
            hd.no_take_order_num,
            hd.take_order_num,
            hd.cancel_after_take_num
        FROM
            (
                SELECT
                    dt,
                    geoToH3Address(lat, lng,7) as h3_id,
                    count(order_id) as today_order_num,
                    count(if(status=4 or status=5, order_id, null)) as today_finished_order_num
                FROM
                    order_data
                GROUP BY
                    dt,
                    geoToH3Address(lat, lng,7)

            )
            td
            LEFT JOIN
            (
                SELECT
                    dt,
                    geoToH3Address(start_location['lat'],start_location['lng'],7) as h3_id,
                    count(distinct(if(status=0, order_id, null))) as order_num,
                    count(distinct(if(status=4 or status=5, order_id, null))) as finished_order_num,
                    count(distinct(if(status=6 and driver_id=0, order_id, null))) as no_take_order_num,
                    count(distinct(if(status=1 and driver_id>0, order_id, null))) as take_order_num,
                    count(distinct(if(status=6 and driver_id>0, order_id, null))) as cancel_after_take_num
                FROM
                    oride_source.user_order
                WHERE
                    dt='{{ ds }}' AND hour='{{ execution_date.strftime("%H") }}'
                GROUP BY
                    dt,
                    geoToH3Address(start_location['lat'],start_location['lng'],7)
            ) hd ON hd.dt=td.dt and hd.h3_id=td.h3_id
        """,
    schema='dashboard',
    dag=dag)

clear_mysql_data = MySqlOperator(
    task_id='clear_mysql_data',
    sql="""
        DELETE FROM oride_transportation_capacity_control WHERE dt = '{{ ds }}' AND hour='{{ execution_date.strftime("%H") }}'
    """,
    mysql_conn_id='mysql_bi',
    dag=dag)

export_to_mysql = HiveToMySqlTransfer(
    task_id='export_to_mysql',
    sql="""
        SELECT
            *
        FROM
           dashboard.oride_transportation_capacity_control
        WHERE
            dt = '{{ ds }}' AND hour='{{ execution_date.strftime("%H") }}'
        """,
    mysql_conn_id='mysql_bi',
    mysql_table='oride_transportation_capacity_control',
    dag=dag)

refresh_impala = ImpalaOperator(
    task_id = 'refresh_impala',
    hql="""\
        REFRESH oride_transportation_capacity_control;
    """,
    schema='dashboard',
    priority_weight=50,
    dag=dag
)

add_partitions >> insert_oride_transportation_capacity_control
create_oride_transportation_capacity_control >> insert_oride_transportation_capacity_control
insert_oride_transportation_capacity_control >> refresh_impala
insert_oride_transportation_capacity_control >> export_to_mysql
clear_mysql_data >> export_to_mysql


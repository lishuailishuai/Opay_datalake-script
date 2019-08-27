import airflow
from airflow.operators.hive_operator import HiveOperator
from datetime import datetime, timedelta
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime(2019, 7, 24),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'dm_oride_passenge_ocash_api_cube_d',
    schedule_interval="30 01 * * *",
    default_args=args)

create_dm_oride_passenge_ocash_api_cube_d = HiveOperator(
    task_id='create_dm_oride_passenge_ocash_api_cube_d',
    hql="""
        CREATE external TABLE IF NOT EXISTS `dm_oride_passenge_ocash_api_cube_d`(
            `user_id` bigint comment '乘客id',
            `city_id` int comment '城市id',
            `serv_type` tinyint COMMENT '1 专车 2 快车',
            `pay_mode` tinyint comment '支付方式 1线上 2线下',
            `completed_num` int comment '完单数',
            `amount` decimal(10,2) comment '支付金额'
        )
        PARTITIONED BY (
            `dt` string
        )
        STORED AS PARQUET
        LOCATION
        'ufile://opay-datalake/oride/oride_dw/dm_oride_passenge_ocash_api_cube_d';
        """,
    schema='oride_dw',
    dag=dag)

insert_dm_oride_passenge_ocash_api_cube_d = HiveOperator(
    task_id='insert_dm_oride_passenge_ocash_api_cube_d',
    hql="""
        ALTER TABLE dm_oride_passenge_ocash_api_cube_d DROP IF EXISTS PARTITION (dt = '{{ ds }}');
        -- 订单信息
        with order_data as (
            SELECT
              *
            FROM
              oride_db.data_order
            WHERE
              dt='{{ ds }}' and from_unixtime(create_time, 'yyyy-MM-dd')=dt and user_id>0
        ),
        -- 支付信息
        pay_data as (
            SELECT
              *,
              if(mode=2 or mode=3, 1, 2) as pay_mode
            FROM
              oride_db.data_order_payment
            WHERE
              dt='{{ ds }}' and status=1
        ),
        -- 司机信息
        driver_data as (
            SELECT
              *
            FROM
              oride_db.data_driver_extend
            WHERE
              dt='{{ ds }}'
        )
        INSERT OVERWRITE TABLE dm_oride_passenge_ocash_api_cube_d PARTITION (dt = '{{ ds }}')
        SELECT
            od.user_id,
            dd.city_id,
            dd.serv_type,
            pd.pay_mode,
            COUNT(od.id) as completed_num,
            SUM(pd.amount) as amount
        FROM
            order_data od
            INNER JOIN pay_data pd ON pd.id=od.id
            INNER JOIN driver_data dd ON od.driver_id=dd.id
        GROUP BY
            od.user_id,
            dd.city_id,
            dd.serv_type,
            pd.pay_mode
        """,
    schema='oride_dw',
    dag=dag)


clear_mysql_data=MySqlOperator(
    task_id='clear_mysql_data',
    sql="""
            DELETE FROM oride_data.data_user_day_stats WHERE dt='{{ ds_nodash }}'
        """,
    dag=dag)

export_to_mysql=HiveToMySqlTransfer(
    task_id='export_to_mysql',
    sql="""
            SELECT
                null as id,
                user_id,
                from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'), 'yyyyMMdd'),
                pay_mode,
                city_id,
                serv_type,
                completed_num,
                amount
            FROM
                oride_dw.dm_oride_passenge_ocash_api_cube_d
            WHERE
                  dt='{{ ds }}'
        """,
    mysql_table='data_user_day_stats',
    dag=dag)

create_dm_oride_passenge_ocash_api_cube_d >> insert_dm_oride_passenge_ocash_api_cube_d >> clear_mysql_data >> export_to_mysql


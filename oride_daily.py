import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator

args = {
    'owner': 'root',
    'start_date': datetime(2019, 5, 20),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    'oride_daily',
    schedule_interval="30 02 * * *",
    default_args=args)

create_oride_driver_overview  = HiveOperator(
    task_id='create_oride_driver_overview',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_driver_overview(
          driver_id bigint,
          order_status int,
          payment_mode int,
          payment_status int,
          order_num_total bigint,
          price_total decimal(10,2),
          distance_total bigint,
          duration_total bigint
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET

    """,
    schema='dashboard',
    dag=dag)

insert_oride_driver_overview  = HiveOperator(
    task_id='insert_oride_driver_overview',
    hql="""
        INSERT OVERWRITE TABLE oride_driver_overview PARTITION (dt='{{ ds }}')
        SELECT
            o.driver_id,
            o.status,
            p.mode,
            p.status,
            count(o.id),
            sum(o.price),
            sum(o.distance),
            sum(o.duration)
        FROM
            oride_source.db_data_order o
            INNER JOIN
            oride_source.db_data_order_payment p on o.id=p.id and o.dt=p.dt
        WHERE
            o.dt = '{{ ds }}' and from_unixtime(o.create_time,'yyyy-MM-dd')='{{ ds }}'
        GROUP BY
            o.driver_id, o.status, p.mode, p.status
    """,
    schema='dashboard',
    dag=dag)

create_oride_user_overview  = HiveOperator(
    task_id='create_oride_user_overview',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_user_overview(
          user_id bigint,
          is_new boolean,
          order_status int,
          payment_mode int,
          payment_status int,
          order_num_total bigint,
          price_total decimal(10,2),
          distance_total bigint,
          duration_total bigint
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET

    """,
    schema='dashboard',
    dag=dag)

insert_oride_user_overview  = HiveOperator(
    task_id='insert_oride_user_overview',
    hql="""
        INSERT OVERWRITE TABLE oride_user_overview PARTITION (dt='{{ ds }}')
        SELECT
            u.user_id,
            u.is_new,
            od.order_status,
            od.payment_mode,
            od.payment_status,
            od.order_num_total,
            od.price_total,
            od.distance_total,
            od.duration_total
        FROM
            (
                SELECT
                    user_id,
                    max(is_new) as is_new
                FROM
                    oride_source.user_login
                WHERE
                    dt='{{ ds }}'
                GROUP BY
                    user_id
            ) u
            LEFT JOIN (
                SELECT
                    o.user_id as user_id,
                    o.status as order_status,
                    p.mode as payment_mode,
                    p.status as payment_status,
                    count(o.id) as order_num_total,
                    sum(o.price) as price_total,
                    sum(o.distance) as distance_total,
                    sum(o.duration) as duration_total
                FROM
                    oride_source.db_data_order o
                    INNER JOIN
                    oride_source.db_data_order_payment p on o.id=p.id and o.dt=p.dt
                WHERE
                    o.dt = '{{ ds }}' and from_unixtime(o.create_time,'yyyy-MM-dd')='{{ ds }}'
                GROUP BY
                    o.user_id, o.status, p.mode, p.status
            ) od ON u.user_id = od.user_id
    """,
    schema='dashboard',
    dag=dag)

# 给用户打标签
create_oride_user_label  = HiveOperator(
    task_id='create_oride_user_lable',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_user_label(
          user_id bigint,
          lab_new_user boolean,
          lab_login_without_orders boolean,
          lab_login_have_orders boolean,
          lab_cancel_ge_finish boolean
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET
    """,
    schema='dashboard',
    dag=dag)

# 1. 新用户, 2, 1周有多次登录但是没有打过车，3，一周多次登录 多次打车，4，取消大于完成
insert_oride_user_label  = HiveOperator(
    task_id='insert_oride_user_lable',
    hql="""
        ALTER TABLE oride_user_label DROP IF EXISTS PARTITION (dt='{{ ds }}');
        -- 一周登录情况 汇总登录次数
        WITH week_login as (
            SELECT
              user_id,
              count(distinct dt) as login_num
            FROM
              oride_source.user_login
            WHERE
              dt BETWEEN '{{ macros.ds_add(ds, -7) }}' AND '{{ ds }}'
            GROUP BY
              user_id
        ),
        -- 一周订单情况 统计完成取消
        week_order as (
            SELECT
              user_id,
              count(if(status=6, 1, null)) as cancel_num,
              count(if(status=5, 1, null)) as finished_num
            FROM
              oride_source.db_data_order
            WHERE
              dt = '{{ ds }}' AND from_unixtime(create_time,'yyyy-MM-dd') BETWEEN '{{ macros.ds_add(ds, -7) }}' AND '{{ ds }}'
            GROUP BY
              user_id
        )
        INSERT OVERWRITE TABLE oride_user_label PARTITION (dt='{{ ds }}')
        SELECT
          ue.id,
          if(datediff('{{ ds }}', from_unixtime(ue.register_time,'yyyy-MM-dd')) < 7, true, false),
          if(nvl(wl.login_num,0)>1 and nvl(wo.finished_num, 0) < 1, true, false),
          if(nvl(wl.login_num,0)>1 and nvl(wo.finished_num, 0) > 1, true, false),
          if(nvl(wo.cancel_num, 0) > nvl(wo.finished_num,0), true, false)
        FROM
          oride_source.db_data_user_extend ue
          LEFT JOIN week_login wl ON wl.user_id = ue.id
          LEFT JOIN week_order wo ON wo.user_id = ue.id
        WHERE
          ue.dt='{{ ds }}'
    """,
    schema='dashboard',
    dag=dag)

refresh_impala = ImpalaOperator(
    task_id = 'refresh_impala',
    hql="""\
        REFRESH dashboard.oride_driver_overview PARTITION (dt='{{ds}}');
        REFRESH dashboard.oride_user_overview PARTITION (dt='{{ds}}');
        REFRESH dashboard.oride_user_overview PARTITION (dt='{{ds}}');
    """,
    schema='dashboard',
    priority_weight=50,
    dag=dag
)

create_oride_driver_overview >> insert_oride_driver_overview
create_oride_user_overview >> insert_oride_user_overview
insert_oride_driver_overview >> refresh_impala
insert_oride_user_overview >> refresh_impala
create_oride_user_label >> insert_oride_user_label
insert_oride_user_label >> refresh_impala
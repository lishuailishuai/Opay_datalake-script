import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from airflow.operators.python_operator import PythonOperator
from utils.connection_helper import get_hive_cursor
from airflow.utils.email import send_email
import csv

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime(2019, 6, 4),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_operation_support',
    schedule_interval="30 02 * * *",
    default_args=args)

create_oride_order_detail = HiveOperator(
    task_id='create_oride_order_detail',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_order_detail (
          order_id bigint,
          user_id bigint,
          phone_number string,
          first_name string,
          last_name string,
          is_new_customer tinyint,
          is_order_success tinyint,
          is_offline_order tinyint,
          score tinyint
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET

    """,
    schema='dashboard',
    dag=dag)

insert_oride_order_detail  = HiveOperator (
    task_id='insert_oride_order_detail',
    hql="""
        INSERT OVERWRITE TABLE oride_order_detail PARTITION (dt='{{ ds }}')
        SELECT
            do.id as order_id,
            do.user_id,
            du.phone_number,
            du.first_name,
            du.last_name,
            if(do.id=ufo.first_order_id, 1, 0) as is_new_customer,
            if(do.status=5, 1, 0) as is_order_success,
            if(dop.mode=1, 1, 0) as is_offline_order,
            duc.score
        FROM
            oride_db.data_order do
            INNER JOIN
                oride_db.data_user du on du.id = do.user_id AND du.dt=do.dt
            INNER JOIN
            (
               select dt, user_id, min(id) as first_order_id from oride_db.data_order WHERE dt = '{{ ds }}' group by user_id,dt
            ) ufo ON ufo.user_id = do.user_id AND ufo.dt=do.dt
            LEFT JOIN
                oride_db.data_order_payment dop on  dop.dt=do.dt AND dop.id=do.id
            LEFT JOIN
                oride_db.data_user_comment duc ON duc.dt=do.dt AND duc.order_id=do.id
        WHERE
            do.dt = '{{ ds }}' and from_unixtime(do.create_time, 'yyyy-MM-dd') = do.dt
    """,
    schema='dashboard',
    dag=dag)

"""
def send_order_detail_email(ds, **kwargs):
    cursor = get_hive_cursor()
    query = '''
        SELECT
            *
        FROM
          dashboard.oride_order_detail
        WHERE
          dt='{dt}'
    '''.format(dt=ds)
    cursor.execute(query)
    rows = cursor.fetchall()
    headers = [
        'order_id',
        'user_id',
        'phone_number',
        'first_name',
        'last_name',
        'is_new_customer',
        'is_order_success',
        'is_offline_order',
        'score',
        'date'
    ]
    file_name = '/tmp/oride_order_detail_{dt}.csv'.format(dt=ds)
    with open(file_name, mode='w') as f:
        f_csv = csv.writer(f)
        f_csv.writerow(headers)
        f_csv.writerows(rows)
    # send mail
    email_to = [
        'zhenqian.zhang@opay-inc.com',
        'chingon.cheng@opay-inc.com',
        'ting.lei@opay-inc.com'
    ]
    email_subject = 'oride_order_detail_{dt}'.format(dt=ds)
    send_email(email_to, email_subject, '', [file_name])

order_detail_email = PythonOperator(
    task_id='order_detail_email',
    python_callable=send_order_detail_email,
    provide_context=True,
    dag=dag
)
"""

create_oride_overview = HiveOperator(
    task_id='create_oirde_overview',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_overview (
          dt string,
          new_users int,
          dau int,
          order_num int,
          completed_order_num int,
          canceled_order_num int,
          order_amount int,
          completed_order_amount int,
          canceled_order_amount int,
          new_user_order_num int,
          new_user_completed_order_num int,
          new_user_canceled_order_num int
        )
        STORED AS PARQUET
    """,
    schema='dashboard',
    dag=dag)

insert_oride_overview = HiveOperator(
    task_id='insert_oirde_overview',
    hql="""
        -- 删除数据
        INSERT OVERWRITE TABLE oride_overview
        SELECT
            *
        FROM
           oride_overview
        WHERE
            dt != '{{ ds }}';
        -- 插入数据
        with user_data as (
            select
                dt,
                count(*) as dau,
                count(if(is_new=true, user_id, null)) as new_users
            from
                dashboard.oride_active_user
            where
                dt='{{ ds }}'
            group by
                dt
        ),
        order_data as  (
            SELECT
                dt,
                count(id) as order_num,
                count(if(status=5, id, null)) as completed_order_num,
                count(if(status!=5, id, null)) as canceled_order_num,
                sum(price) as order_amount,
                sum(if(status=5, price, 0)) as completed_order_amount,
                sum(if(status!=5, price, 0)) as canceled_order_amount
            FROM
                oride_db.data_order
            WHERE
                dt='{{ ds }}' and from_unixtime(create_time, 'yyyy-MM-dd') = dt
            GROUP BY dt
        ),
        new_user_order_data as (
            SELECT
                do.dt,
                count(do.id) as new_user_order_num,
                count(if(do.status=5, id, null)) as new_user_completed_order_num,
                count(if(do.status!=5, id, null)) as new_user_canceled_order_num
            FROM
                oride_db.data_order do
                inner join dashboard.oride_active_user au ON au.user_id=do.user_id AND au.dt=do.dt
            WHERE
                au.is_new=true AND do.dt='{{ ds }}' AND from_unixtime(do.create_time, 'yyyy-MM-dd') = do.dt
            group by do.dt
        )
        INSERT INTO TABLE oride_overview
        SELECT
            ud.dt,
            ud.new_users,
            ud.dau,
            od.order_num,
            od.completed_order_num,
            od.canceled_order_num,
            od.order_amount,
            od.completed_order_amount,
            od.canceled_order_amount,
            nod.new_user_order_num,
            nod.new_user_completed_order_num,
            nod.new_user_canceled_order_num
        FROM
            user_data ud
            INNER JOIN
            order_data od ON od.dt=ud.dt
            INNER JOIN
            new_user_order_data nod ON nod.dt=ud.dt
    """,
    schema='dashboard',
    dag=dag)


create_oride_user_behavior = HiveOperator(
    task_id='create_oride_user_behavior',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_user_behavior (
          dt string,
          dau int,
          request_order_users int,
          take_order_users int,
          completed_order_users int,
          invited_users int,
          invitee_users int
        )
        STORED AS PARQUET
    """,
    schema='dashboard',
    dag=dag)

insert_oride_user_behavior = HiveOperator(
    task_id='insert_oride_user_behavior',
    hql="""
        -- 删除数据
        INSERT OVERWRITE TABLE oride_user_behavior
        SELECT
            *
        FROM
           oride_user_behavior
        WHERE
            dt != '{{ ds }}';
        -- 插入数据
        with user_data as (
            select
                dt,
                count(*) as dau
            from
                dashboard.oride_active_user
            where
                dt='{{ ds }}'
            group by
                dt
        ),
        order_data as  (
            SELECT
                dt,
                count(distinct user_id) as request_order_users,
                count(distinct if(take_time > 0, user_id, null)) as take_order_users,
                count(distinct if(status =5, user_id, null)) as completed_order_users
            FROM
                oride_db.data_order
            WHERE
                dt='{{ ds }}' and from_unixtime(create_time, 'yyyy-MM-dd') = dt
            GROUP BY dt
        ),
        invite_data as (
            SELECT
                dt,
                count(distinct uid) as invited_users,
                count(distinct invitee_id) as invitee_users
            FROM
                oride_db.data_invite
            WHERE
                dt='{{ ds }}' and from_unixtime(`timestamp`, 'yyyy-MM-dd') = dt
            GROUP BY dt
        )
        INSERT INTO TABLE oride_user_behavior
        SELECT
            ud.dt,
            ud.dau,
            od.request_order_users,
            od.take_order_users,
            od.completed_order_users,
            id.invited_users,
            id.invitee_users
        FROM
            user_data ud
            INNER JOIN
            order_data od ON od.dt=ud.dt
            INNER JOIN
            invite_data id ON id.dt=ud.dt
    """,
    schema='dashboard',
    dag=dag)


refresh_impala = ImpalaOperator(
    task_id = 'refresh_impala',
    hql="""\
        REFRESH dashboard.oride_order_detail PARTITION (dt='{{ds}}');
        REFRESH dashboard.oride_overview;
        REFRESH dashboard.oride_user_behavior;
    """,
    schema='dashboard',
    priority_weight=50,
    dag=dag
)

create_oride_order_detail >> insert_oride_order_detail >> refresh_impala
create_oride_overview >> insert_oride_overview >> refresh_impala
create_oride_user_behavior >> insert_oride_user_behavior >> refresh_impala

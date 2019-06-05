import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from airflow.operators.python_operator import PythonOperator
from utils.connection_helper import get_hive_cursor
from airflow.utils.email import send_email
import csv

args = {
    'owner': 'root',
    'start_date': datetime(2019, 6, 4),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
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

refresh_impala = ImpalaOperator(
    task_id = 'refresh_impala',
    hql="""\
        REFRESH dashboard.oride_order_detail PARTITION (dt='{{ds}}');
    """,
    schema='dashboard',
    priority_weight=50,
    dag=dag
)

create_oride_order_detail >> insert_oride_order_detail >> order_detail_email >> refresh_impala

import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.models import Variable
from impala.dbapi import connect
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'root',
    'start_date': datetime(2019, 4, 20),
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    'ofood_daily',
    schedule_interval="30 01 * * *",
    default_args=args)

insert_ofood_active_user = HiveOperator(
    hql="""
        INSERT OVERWRITE TABLE ofood_active_user PARTITION (dt='{{ ds }}')
        SELECT
            distinct uid
        FROM
            ofood_source.user_login
        WHERE dt = '{{ ds }}'
    """,
    schema='dashboard',
    task_id='insert_ofood_active_user',
    dag=dag)

insert_ofood_active_user_retention = HiveOperator(
    hql="""
        INSERT OVERWRITE TABLE ofood_active_user_retention PARTITION (dt='{{ ds }}')
        SELECT
            b.dt,
            DATEDIFF('{{ ds }}', b.dt) AS period,
            NVL(a.retained_users, 0) AS retained_users,
            b.cohort_size
        FROM
            (
                SELECT
                    n.dt AS dt,
                    COUNT(*) AS retained_users
                FROM
                    ofood_active_user au, ofood_active_user n
                WHERE
                    au.uid = n.uid
                    AND au.dt = '{{ ds }}'
                    AND n.dt >= '{{ macros.ds_add(ds, -30) }}'
                    AND n.dt < '{{ ds }}'
                GROUP BY
                    n.dt
            ) a
            RIGHT OUTER JOIN
            (
                SELECT
                    dt,
                    count(*) as cohort_size
                FROM
                    ofood_active_user
                WHERE
                    dt >= '{{ macros.ds_add(ds, -30) }}' AND dt < '{{ ds }}'
                GROUP BY
                    dt
            ) b ON a.dt = b.dt
    """,
    schema='dashboard',
    task_id='insert_ofood_active_user_retention',
    dag=dag)

IMPALA_QUERY = [
    "REFRESH dashboard.ofood_active_user",
    "REFRESH dashboard.ofood_active_user_retention"
]

def impala_query(ds, **kwargs):
    conn = connect(host=Variable.get("IMPALA_URL"), port=21050)
    cur = conn.cursor()
    for sql in IMPALA_QUERY:
        cur.execute(sql)

refresh_impala = PythonOperator(
    task_id='refresh_impala',
    provide_context=True,
    python_callable=impala_query,
    dag=dag
)

insert_ofood_active_user >> insert_ofood_active_user_retention
insert_ofood_active_user >> refresh_impala
insert_ofood_active_user_retention >> refresh_impala
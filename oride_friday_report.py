import airflow
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 6, 15),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'friday_report',
    schedule_interval="30 08 * * 5",
    default_args=args)


create_csv_file = BashOperator(
    task_id='create_csv_file',
    bash_command='''
        log_path="/data/app_log"
        dt="{{ ds }}"

        # 导出数据到文件
        # 周五报表
        friday_report_sql=`cat << EOF
            select 
            u.dt ,
            u.dau DAU,
            u.dnu DNU,
            u.dnu_rate ,
            o.ride_num ,
            o.on_ride_num ,
            d.new_customer_rate ,
            o.on_ride_rate ,
            o.on_ride_driver_num ,
            on_ride_driver_avg ,
            on_ride_user_num ,
            take_time_avg
            from
            (
                select 
                dt dt,
                count(user_id) dau,
                count(if(is_new = true,user_id,null)) dnu,
                count(if(is_new = true,user_id,null))/(count(user_id)) dnu_rate
                from dashboard.oride_active_user
                group by dt
            ) u
            join 
            (
                select 
                from_unixtime(create_time,'yyyy-MM-dd') dt,
                count(id) ride_num,
                count(if(status = 4 or status = 5,id,null)) on_ride_num,
                count(if(status = 4 or status = 5,id,null))/count(id)  on_ride_rate,
                count(distinct(if(status = 4 or status = 5,driver_id,null))) on_ride_driver_num,
                count(if(status = 4 or status = 5,id,null))/count(distinct(if(status = 4 or status = 5,driver_id,null))) on_ride_driver_avg,
                count(distinct(if(status = 4 or status = 5,user_id,null))) on_ride_user_num,
                sum(if(take_time <> 0,take_time - create_time,0))/count(if(driver_id > 0,id,null)) take_time_avg
                from oride_db.data_order where dt = '${dt}'
                group by from_unixtime(create_time,'yyyy-MM-dd')
            ) o on u.dt = o.dt
            join 
            (
                select 
                dt dt,
                count(distinct(if(is_order_success = 1 and is_new_customer = 1,user_id,null)))/count(distinct(if(is_order_success = 1,user_id,null)))  new_customer_rate  
                from dashboard.oride_order_detail 
                group by dt
                ) d on d.dt = u.dt
                order by u.dt
            ;
            EOF
`
        hive -e "set hive.cli.print.header=true; ${friday_report_sql}" | sed 's/[\t]/,/g'  > ${log_path}/tmp/friday_report_${dt}.csv
    ''',
    dag=dag,
)

def send_csv_file(ds, **kwargs):
    name_list = [
        'friday_report'
    ]
    file_list = []
    for name in name_list:
        file_list.append("/data/app_log/tmp/%s_%s.csv" % (name, ds))

    # send mail
    email_to = [
        'zhenqian.zhang@opay-inc.com',
        'nan.li@opay-inc.com',
        'song.zhang@opay-inc.com',

    ]
    email_subject = 'friday_report_{dt}'.format(dt=ds)
    send_email(email_to, email_subject, '', file_list)

send_file_email = PythonOperator(
    task_id='send_file_email',
    python_callable=send_csv_file,
    provide_context=True,
    dag=dag
)

create_csv_file >> send_file_email
import airflow
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email

args = {
    'owner': 'root',
    'start_date': datetime(2019, 6, 14),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    'capacity_dispatch_daily',
    schedule_interval="30 02 * * *",
    default_args=args)

import_log_file = BashOperator(
    task_id='import_log_file',
    bash_command='''
        log_path="/data/app_log"
        dt="{{ ds_nodash }}"
        mkdir -p ${log_path}/${dt}
        # pull log file
        scp -P 622 root@124.156.118.128:/data/app/dispatcher/logs/${dt}.log ${log_path}/${dt}/gw1.log
        scp -P 2522 root@124.156.118.128:/data/app/dispatcher/logs/${dt}.log ${log_path}/${dt}/gw2.log
        scp -P 22722 root@124.156.118.128:/data/app/dispatcher/logs/${dt}.log ${log_path}/${dt}/gw3.log
    ''',
    dag=dag,
)

create_csv_file = BashOperator(
    task_id='create_csv_file',
    bash_command='''
        log_path="/data/app_log"
        dt="{{ ds_nodash }}"

        # 圈选不到司机
        cd ${log_path}/${dt}
        grep -R "no dax found for order" * | awk '{split($13,a,"("); print $3" "substr($4,0,8)","a[1]}' > ${log_path}/tmp/order_no_found_driver_${dt}.log
        hive -e "LOAD DATA LOCAL INPATH '${log_path}/tmp/order_no_found_driver_${dt}.log' OVERWRITE INTO TABLE test_db.order_no_found_driver PARTITION (dt='${dt}');"

        # 圈选到司机
        grep -R "found dax count" * | awk '{split($12,a,"("); print $3" "substr($4,0,8)","$9","a[1]}' > ${log_path}/tmp/order_found_count_${dt}.log
        hive -e "LOAD DATA LOCAL INPATH '${log_path}/tmp/order_found_count_${dt}.log' OVERWRITE INTO TABLE test_db.order_found_count PARTITION (dt='${dt}');"

        # 过滤司机
        grep -R "dax filtered because" * | awk '{if($9 != "[not_in_service_mode]|"){split($12,a,"(");split($15,b,"("); print $3" "substr($4,0,8)","$9","a[1]","b[1]} }' > ${log_path}/tmp/order_filtered_because_${dt}.log
        grep -R "dax filtered because" * | awk '{if($9 == "[not_in_service_mode]|"){split($11,a,"(");split($14,b,"("); print $3" "substr($4,0,8)","$9","a[1]","b[1]} }' >> ${log_path}/tmp/order_filtered_because_${dt}.log
        hive -e "LOAD DATA LOCAL INPATH '${log_path}/tmp/order_filtered_because_${dt}.log' OVERWRITE INTO TABLE test_db.order_filtered_because PARTITION (dt='${dt}');"

        # 播报司机
        grep -R "order assign" *  | awk -F '|' '{n=split($3,nn,"} {");split($1,a," ");match($2, /{ID:([0-9]+)/, b);print a[3]" "substr(a[4],0,8)","b[1]","n}' > ${log_path}/tmp/order_assign_${dt}.log
        hive -e "LOAD DATA LOCAL INPATH '${log_path}/tmp/order_assign_${dt}.log' OVERWRITE INTO TABLE test_db.order_assign PARTITION (dt='${dt}');"

        # 推单日志
        cat * | grep "push message"  | grep "/driver/order" | awk '{match($0, /.+role: 2:([0-9]+).+"order":{"id":([0-9]+)./,a);print $3" "substr($4,0,8)"\t"a[1]"\t"a[2]}' > ${log_path}/tmp/push_message_${dt}.log

        # load 到hive 过程
        hive -e "load data local inpath '${log_path}/tmp/push_message_${dt}.log' overwrite into table test_db.push_message partition(dt='${dt}');"

        # 导出数据到文件
        # 圈选不到司机
        order_no_found_driver_sql=`cat << EOF
            select
                dt,
                order_id,
                rank() over(partition by order_id order by unix_timestamp(timestr, 'yyyy-MM-dd HH:mm:ss') asc ) as rank_num
            from
                test_db.order_no_found_driver
            where
                dt='${dt}'
EOF
`
        hive -e "set hive.cli.print.header=true; ${order_no_found_driver_sql}" | sed 's/[\t]/,/g'  > ${log_path}/tmp/order_no_found_driver_${dt}.csv

        # 轮播数据
        order_assgin_sql=`cat << EOF
           select
              ofc.dt,
              ofc.order_id,
              ofc.rank_num,
              max(ofc.count_str) as count_str,
              sum(if(ofb.because='[assigned_another_job]', 1, 0)) as assigned_another_job_num,
              sum(if(ofb.because='[not_in_service_mode]|', 1, 0)) as not_in_service_mode_num,
              sum(if(ofb.because='[not_idle]', 1, 0)) as not_idle_num,
              sum(if(ofb.because='[assigned_this_order_before]', 1, 0)) as assigned_this_order_before,
              max(oa.driver_num) as assign_driver_num,
              max(unix_timestamp(oa.timestr, 'yyyy-MM-dd HH:mm:ss')) as assign_time,
              max(ofc.driver_id) as driver_id,
              max(ofc.take_time) as take_time
            from
            (
                select
                    a.dt,
                    a.order_id,
                    a.count_str,
                    dense_rank() over(partition by order_id order by unix_timestamp(timestr, 'yyyy-MM-dd HH:mm:ss') asc ) as rank_num,
                    rank() over(partition by order_id order by unix_timestamp(timestr, 'yyyy-MM-dd HH:mm:ss') desc ) as top_rank,
                    if (rank() over(partition by order_id order by unix_timestamp(timestr, 'yyyy-MM-dd HH:mm:ss') desc ) =1, b.driver_id, 0) as driver_id,
                    if (rank() over(partition by order_id order by unix_timestamp(timestr, 'yyyy-MM-dd HH:mm:ss') desc ) =1, b.take_time, 0) as take_time,
                    a.timestr
                from test_db.order_found_count a
                left join oride_db.data_order b ON b.id=a.order_id and b.dt='{{ ds }}'
                where a.dt='${dt}'
            ) ofc
            left join
            (
                select
                    dt,
                    order_id,
                    because,
                    timestr
                from test_db.order_filtered_because
            ) ofb on ofb.dt=ofc.dt and ofb.order_id=ofc.order_id and ofb.timestr=ofc.timestr
            left join
            (
               select
                    dt,
                    timestr,
                    order_id,
                    driver_num
                from
                    test_db.order_assign
            ) oa on oa.dt=ofc.dt and oa.order_id=ofc.order_id and oa.timestr=ofc.timestr
            where ofc.dt='${dt}'
            group by
              ofc.dt,
              ofc.order_id,
              ofc.rank_num
EOF
`
        hive -e "set hive.cli.print.header=true; ${order_assgin_sql}" | sed 's/[\t]/,/g'  > ${log_path}/tmp/order_assgin_${dt}.csv

        # 平均抢单时长
        avg_take_time_sql=`cat << EOF
        select
         from_unixtime(o.create_time,'yyyy-MM-dd'),
         o.driver_id,
        sum(if(o.take_time >0,o.take_time - unix_timestamp(t.max_timestr),0))/count(if(o.take_time >0,o.id,null)),
        sum(if(o.take_time >0,o.take_time - unix_timestamp(t.min_timestr),0))/count(if(o.take_time >0,o.id,null))
        from
        (select id,driver_id,status,take_time,create_time
        from
        oride_db.data_order
        where dt = '{{ ds }}' and from_unixtime(create_time,'yyyy-MM-dd') between '{{macros.ds_add(ds, -2)}}' and '{{ ds }}'
        ) o join
        (
        select order_id ,driver_id,max(timestr) max_timestr,min(timestr) min_timestr
        from
        test_db.push_message
        where dt between '{{macros.ds_format(macros.ds_add(ds, -2), "%Y-%m-%d", "%Y%m%d")}}' and '${dt}'
        group by order_id ,driver_id
        ) t on o.id = t.order_id and o.driver_id = t.driver_id
        group by from_unixtime(o.create_time,'yyyy-MM-dd'),o.driver_id;
EOF
`
        hive -e "set hive.cli.print.header=true; ${avg_take_time_sql}" | sed 's/[\t]/,/g'  > ${log_path}/tmp/avg_take_time_${dt}.csv

        # 司机被推单数据
        driver_push_sql=`cat << EOF
        select
            to_date(timestr),
            driver_id,
            count(order_id) order_num,
            count(distinct(order_id)) order_num_dis
        from
            test_db.push_message
        where dt = '${dt}'
        group by to_date(timestr),driver_id
EOF
`
        hive -e "set hive.cli.print.header=true; ${driver_push_sql}" | sed 's/[\t]/,/g'  > ${log_path}/tmp/driver_push_${dt}.csv

        # 司机抢单量，完成量
        driver_finish_order_sql=`cat << EOF
        select
            from_unixtime(create_time,'yyyy-MM-dd') as create_dt,
            driver_id,
            count(if(driver_id is not null,id,null)),
            count(if(status = 4 or status = 5,id,null))
        from
            oride_db.data_order
        where
            dt = '{{ ds }}' and from_unixtime(create_time,'yyyy-MM-dd') between '{{ macros.ds_add(ds, -2) }}' and '{{ ds }}'
        group by from_unixtime(create_time,'yyyy-MM-dd'),driver_id
        order by create_dt desc,driver_id
EOF
`
        hive -e "set hive.cli.print.header=true; ${driver_finish_order_sql}" | sed 's/[\t]/,/g'  > ${log_path}/tmp/driver_finish_order_${dt}.csv

        # daily下单量、完单量等信息
        daily_order_sql=`cat << EOF
        select
            from_unixtime(create_time,'yyyy-MM-dd'),
            count(id),
            count(if(status = 5 or status = 4,id,null)),
            count(if(status = 5 or status = 4,id,null))/count(id),
            count(distinct(if(status = 5 or status = 4,driver_id,null))),
            count(if(status = 5 or status = 4,id,null))/count(distinct(if(status = 5 or status = 4,driver_id,null)))
        from
            oride_db.data_order where dt= '{{ ds }}' and from_unixtime(create_time,'yyyy-MM-dd') between '{{ macros.ds_add(ds, -2) }}' and '{{ ds }}'
        group by from_unixtime(create_time,'yyyy-MM-dd');
EOF
`
        hive -e "set hive.cli.print.header=true; ${daily_order_sql}" | sed 's/[\t]/,/g'  > ${log_path}/tmp/daily_order_${dt}.csv

        driver_push_avg_dis_sql=`cat << EOF
        select
            dt,
            count(distinct(order_id))/count(distinct(driver_id))
        from
            test_db.push_message
        where
            dt='${dt}'
        group by dt
        order by dt

EOF
`
        hive -e "set hive.cli.print.header=true; ${driver_push_avg_dis_sql}" | sed 's/[\t]/,/g'  > ${log_path}/tmp/driver_push_avg_dis_${dt}.csv
    ''',
    dag=dag,
)

def send_csv_file(ds_nodash, **kwargs):
    name_list = [
        'avg_take_time',
        'daily_order',
        'driver_finish_order',
        'driver_push',
        'order_assgin',
        'order_no_found_driver',
        'driver_push_avg_dis'
    ]
    file_list = []
    for name in name_list:
        file_list.append("/data/app_log/tmp/%s_%s.csv" % (name, ds_nodash))

    # send mail
    email_to = [
        'zhenqian.zhang@opay-inc.com',
        'nan.li@opay-inc.com',
        'song.zhang@opay-inc.com',

    ]
    email_subject = 'capacity_dispatch_daily_{dt}'.format(dt=ds_nodash)
    send_email(email_to, email_subject, '', file_list)

send_file_email = PythonOperator(
    task_id='send_file_email',
    python_callable=send_csv_file,
    provide_context=True,
    dag=dag
)

import_log_file >> create_csv_file >> send_file_email

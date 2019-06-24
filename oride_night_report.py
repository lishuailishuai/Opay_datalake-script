import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from airflow.operators.bash_operator import BashOperator
from utils.connection_helper import get_hive_cursor, get_db_conf
from airflow.operators.hive_operator import HiveOperator
import logging
from airflow.models import Variable

args = {
    'owner': 'root',
    'start_date': datetime(2019, 6, 17),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_night_report',
    schedule_interval="0 23 * * *",
    default_args=args)

table_list = [
    "data_order",
    "data_order_payment",
    "data_user",
    "data_user_extend",
    "data_driver_extend",
    "data_user_recharge",
]

def send_report_email(tomorrow_ds, **kwargs):
    sql = '''
        with user_data as (
            SELECT
                dt,
                sum(if(from_unixtime(register_time, 'yyyy-MM-dd')=dt, 1, 0)) as new_users,
                sum(if(from_unixtime(login_time, 'yyyy-MM-dd')=dt, 1, 0)) as active_users
            FROM
                oride_db.data_user_extend
            WHERE
                dt='{ds}'
            GROUP BY dt
        ),
        driver_data as (
            SELECT
                dt,
                sum(if(from_unixtime(register_time, 'yyyy-MM-dd')=dt, 1, 0)) as new_drivers,
                count(id) as total_drivers
            FROM
                oride_db.data_driver_extend
            WHERE
                dt='{ds}'
            GROUP BY dt
        ),
        order_data as (
            SELECT
                do.dt,
                COUNT(DISTINCT do.user_id) as request_users,
                COUNT(DISTINCT if(dop.mode=2 or dop.mode=3, do.user_id, NULL)) as online_pay_users,
                COUNT(DISTINCT if(do.driver_id>0, do.driver_id, NULL)) as take_drivers,
                COUNT(DISTINCT if(do.status=4 or do.status=5, do.driver_id, NULL)) as finish_drivers,
                COUNT(do.id) as request_num,
                SUM(if(do.driver_id>0, 1, 0)) as take_num,
                SUM(if(do.status=4 or do.status=5, 1, 0)) as finish_num,
                COUNT(DISTINCT if(dop.mode=2 or dop.mode=3, do.id, NULL)) as online_pay_orders,
                SUM(if(do.take_time>0, do.take_time-do.create_time, 0)) as total_take_time,
                SUM(if(do.pickup_time>0, do.pickup_time-do.take_time, 0)) as total_pickup_time,
                SUM(if(do.pickup_time>0, 1, 0)) as pickup_num
            FROM
                oride_db.data_order do
                LEFT JOIN oride_db.data_order_payment dop ON dop.id=do.id AND dop.dt=do.dt
            WHERE
                do.dt='{ds}' AND from_unixtime(do.create_time, 'yyyy-MM-dd')=do.dt
            GROUP BY do.dt
        ),
        recharge_data as (
            SELECT
                dt,
                count(if(from_unixtime(create_time, 'yyyy-MM-dd')=dt, id, null)) as td_recharge_times,
                sum(if(from_unixtime(create_time, 'yyyy-MM-dd')=dt, amount, 0)) as td_recharge_amount,
                count(id) as recharge_times,
                sum(amount) as recharge_amount,
                sum(if(from_unixtime(create_time, 'yyyy-MM-dd')=dt, bonus, 0)) as td_recharge_bonus,
                sum(bonus) as recharge_bonus
            FROM
                oride_db.data_user_recharge
            WHERE
                dt='{ds}' AND status=1
            GROUP BY dt
        )
        SELECT
            od.request_users,
            ud.active_users,
            ud.new_users,
            od.online_pay_users,
            dd.total_drivers,
            dd.new_drivers,
            od.take_drivers,
            od.finish_drivers,
            od.request_num,
            od.take_num,
            od.finish_num,
            od.online_pay_orders,
            od.total_take_time,
            od.total_pickup_time,
            od.pickup_num,
            rd.td_recharge_times,
            rd.td_recharge_amount,
            rd.recharge_times,
            rd.recharge_amount,
            rd.td_recharge_bonus,
            rd.recharge_bonus
        FROM
            user_data ud
            INNER JOIN driver_data dd ON dd.dt=ud.dt
            INNER JOIN order_data od ON od.dt=ud.dt
            LEFT JOIN recharge_data rd on rd.dt=ud.dt
    '''.format(ds=tomorrow_ds)
    cursor = get_hive_cursor()
    logging.info(sql)
    cursor.execute(sql)
    res = cursor.fetchall()
    if len(res)>0 :
        [request_users, active_users, new_users, online_pay_users, total_drivers, new_drivers, take_drivers, finish_drivers, request_num,take_num, finish_num, online_pay_orders, total_take_time, total_pickup_time, pickup_num, td_recharge_times, td_recharge_amount, recharge_times, recharge_amount, td_recharge_bonus, recharge_bonus] = list(res[0])
        html_fmt = '''
        <html>
        <head>
        <title></title>
        <style type="text/css">
            table
            {{
                font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
                border-collapse: collapse;
                margin: 0 auto;
                text-align: left;
            }}
            table td, table th
            {{
                border: 1px solid #cad9ea;
                color: #666;
                height: 30px;
                padding: 5px 10px 5px 5px;
            }}
            table thead th
            {{
                background-color: #4CAF50;
                color: white;
                width: 100px;
            }}
            table tr:nth-child(odd)
            {{
                background: #fff;
            }}
            table tr:nth-child(even)
            {{
                background: #F5FAFA;
            }}
        </style>
        </head>
        <body>
            <table width="95%" class="table">
                <caption>
                    <h2>{dt}</h2>
                </caption>
                <thead>
                    <tr>
                        <th colspan="4" style="text-align: center;">乘客</th>
                        <th colspan="5" style="text-align: center;">司机</th>
                        <th colspan="8" style="text-align: center;">订单</th>
                        <th colspan="6" style="text-align: center;">充值</th>
                    </tr>
                    <tr>
                        <!--乘客-->
                        <th>发单乘客数</th>
                        <th>活跃乘客数</th>
                        <th>新增活跃乘客数</th>
                        <th>线上支付乘客数</th>
                        <!--司机-->
                        <th>累计注册司机数</th>
                        <th>注册司机数</th>
                        <th>抢单司机数</th>
                        <th>完单司机数</th>
                        <th>人均完单数</th>
                        <!--订单-->
                        <th>下单数</th>
                        <th>抢单数</th>
                        <th>完单数</th>
                        <th>应答率</th>
                        <th>完单率</th>
                        <th>线上支持订单数</th>
                        <th>应答时长(平均)</th>
                        <th>接驾时长(平均)</th>
                        <!--充值-->
                        <th>每日充值笔数</th>
                        <th>累计充值笔数</th>
                        <th>每日充值真实金额</th>
                        <th>累计充值真实金额</th>
                        <th>每日充值总金额</th>
                        <th>累计充值总金额</th>
                    </tr>
                </thead>
                <tr>
                    <!--乘客-->
                    <td>{request_users}</td>
                    <td>{active_users}</td>
                    <td>{new_users}</td>
                    <td>{online_pay_users}</td>
                    <!--司机-->
                    <td>{total_drivers}</td>
                    <td>{new_drivers}</td>
                    <td>{take_drivers}</td>
                    <td>{finish_drivers}</td>
                    <td>{finish_vs_driver}</td>
                    <!--订单-->
                    <td>{request_num}</td>
                    <td>{take_num}</td>
                    <td>{finish_num}</td>
                    <td>{take_ratio}%</td>
                    <td>{finish_ratio}%</td>
                    <td>{online_pay_orders}</td>
                    <td>{take_time_avg}</td>
                    <td>{pickup_time_avg}</td>
                    <!--充值-->
                    <td>{td_recharge_times}</td>
                    <td>{recharge_times}</td>
                    <td>{td_recharge_amount}</td>
                    <td>{recharge_amount}</td>
                    <td>{td_recharge_total}</td>
                    <td>{recharge_total}</td>
                </tr>
            </table>
        </body>
        </html>
        '''
        html = html_fmt.format(
            dt=tomorrow_ds,
            request_users=request_users,
            active_users=active_users,
            new_users=new_users,
            online_pay_users=online_pay_users,
            total_drivers=total_drivers,
            new_drivers=new_drivers,
            take_drivers=take_drivers,
            finish_drivers=finish_drivers,
            finish_vs_driver=round(finish_num/finish_drivers, 2),
            request_num=request_num,
            take_num=take_num,
            finish_num=finish_num,
            take_ratio=round(take_num/request_num*100,2),
            finish_ratio=round(finish_num/request_num*100,2),
            online_pay_orders=online_pay_orders,
            take_time_avg=round(total_take_time/take_num),
            pickup_time_avg=round(total_pickup_time/pickup_num),
            td_recharge_times=td_recharge_times,
            td_recharge_amount=td_recharge_amount,
            recharge_times=recharge_times,
            recharge_amount=recharge_amount,
            td_recharge_total=td_recharge_amount+td_recharge_bonus,
            recharge_total=recharge_amount+recharge_bonus
        )
        # send mail
        email_subject = 'oride晚十一点数据快报_{}'.format(tomorrow_ds)
        send_email(Variable.get("oride_night_report_receivers").split(), email_subject, html, mime_charset='utf-8')
        #send_email(['zhenqian.zhang@opay-inc.com'], email_subject, html, mime_charset='utf-8')
        cursor.close()
        return

send_report = PythonOperator(
    task_id='send_report',
    python_callable=send_report_email,
    provide_context=True,
    dag=dag
)

host, port, schema, login, password = get_db_conf('sqoop_db')
for table_name in table_list:
    import_table = BashOperator(
        task_id='import_table_{}'.format(table_name),
        bash_command='''
            #!/usr/bin/env bash
            sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
            --connect "jdbc:mysql://{host}:{port}/{schema}?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8" \
            --username {username} \
            --password {password} \
            --table {table} \
            --target-dir ufile://opay-datalake/oride/db/{table}/dt={{{{ tomorrow_ds }}}}/ \
            --fields-terminated-by "\\001" \
            --lines-terminated-by "\\n" \
            --hive-delims-replacement " " \
            --delete-target-dir \
            --compression-codec=snappy
        '''.format(host=host, port=port, schema=schema, username=login, password=password,table=table_name),
        dag=dag,
    )
    add_partitions = HiveOperator(
        task_id='add_partitions_{}'.format(table_name),
        hql='''
            ALTER TABLE oride_db.{table} ADD IF NOT EXISTS PARTITION (dt = '{{{{ tomorrow_ds }}}}')
        '''.format(table=table_name),
        schema='oride_source',
        dag=dag)

    import_table >> send_report
    add_partitions >> send_report

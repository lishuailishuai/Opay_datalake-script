# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import time
from utils.connection_helper import get_hive_cursor, get_db_conn, get_pika_connection
import smtplib
from email.mime.text import MIMEText
from email.header import Header
work_times = 15 * 3600
driver_online_time_key = "online_time:time:2:{driver_id}:{dt}"
# dt format YYYYmmDD
sender = 'research@opay-inc.com'
password = 'G%4nlD$YCJol@Op'
# receivers = ['zhuohua.chen@opay-inc.com']
receivers = ['lichang.zhang@opay-inc.com', 'jikun.li@opay-inc.com', 'zhi.li@opay-inc.com', 'yudiw@opera.com',
             'fengfeng.ning@opay-inc.com', 'xin.ke@opay-inc.com', 'narku.he@kunlun-inc.com', 'zhimeng.lu@opay-inc.com',
             'shuai.ma@opay-inc.com', 'ao.ren@opay-inc.com', 'haihuan.zou@opay-inc.com', 'chengyangw@opay.team',
             'mengshi.yang@opay-inc.com', 'gao.lv@opay-inc.com', 'zhuohua.chen@opay-inc.com',
             'huacai.yang@opay-inc.com', 'ququ@opay.team', 'hua.guo@opay-inc.com', 'jingtian.he@opay-inc.com',
             'jinsong@opera.com', 'qingchengl@opay.team', 'dehuiw@opay.team', 'dinglun.fan@opay-inc.com',
             'Lei.zheng@opay-inc.com', 'chingon.cheng@opay-inc.com']

part_html1 = """<tr><td class="title_td">{key}</td>"""
part_html2 = """<td class="value_td">{val}</td>"""
part_html2_1 = """<td class="value_1_td">{val}</td>"""
part_html3 = """</tr>"""

css_style = '''
<style type="text/css">
.title_td{border:solid#000 1px;width:300px;overflow:hidden;text-align:center}
.value_td{border:solid#000 1px;width:100px;overflow:hidden;text-align:right}
.value_1_td{border:solid#000 1px;width:100px;overflow:hidden;text-align:center}
.table{border-collapse:collapse;border:none;}
</style>
'''
mail_msg_header = """
<html lang="en" dir="ltr">
  <head>
    <meta charset="utf-8">
    <title></title>
  </head>
  <body>
    <table class="table" cellpadding="5">
      <caption><h4>Oride {dt1} -- {dt2} Daily Report</h4></caption>
"""
mail_msg_tail = '''
    </table>
  </body>
</html>
'''

repair_table_query = '''
MSCK REPAIR TABLE %s
'''

query1 = '''
select count(*) as call_num,
sum(if(status=5,1,0)) as success_num,
sum(price*if(status=5,1,0)) as gmv,
sum(if(driver_id=0,1,0) * if(status=6,1,0)) as cancel_before_dispatching_num,
sum(if(driver_id>0,1,0) * if(status=6,1,0) * if (cancel_role=1,1,0)) as cancel_after_dispatching_by_user_num,
sum(if(driver_id>0,1,0) * if(status=6,1,0) * if (cancel_role=2,1,0)) as cancel_after_dispatching_by_driver_num,
sum(if(pickup_time>=create_time,1,0)) as pickup_num,
sum(if(pickup_time>=create_time,pickup_time-create_time,0)) as pickup_total_time,
sum(if(take_time>=create_time,1,0)) as take_num,
sum(if(take_time>=create_time,take_time-create_time,0)) as take_total_time
FROM oride_db.data_order 
where dt = "{dt}" 
and
create_time BETWEEN unix_timestamp('{dt} 00:00:00') and unix_timestamp('{dt} 23:59:59')
'''

query2 = '''
select count(*) as pay_num,
sum(price) as total_price,
sum(price-amount) as total_c_discount,
sum(if(mode=1,1,0)) as offline_num
FROM 
(
select id from
oride_db.data_order
where dt = "{dt}"
and status=5
and create_time BETWEEN unix_timestamp('{dt} 00:00:00') and unix_timestamp('{dt} 23:59:59')
) a 
join
(
select * from
oride_db.data_order_payment
where dt = "{dt}"
) b
on a.id = b.id
'''

query3 = '''
SELECT 
count(*) as order_num,
sum(amount) as total_reward
FROM 
(
select id from
oride_db.data_order
where dt = "{dt}"
and status=5
and create_time BETWEEN unix_timestamp('{dt} 00:00:00') and unix_timestamp('{dt} 23:59:59')
) a 
join
(
select * from
oride_db.data_driver_reward
where dt = "{dt}"
) b
on a.id = b.order_id
'''

query4 = '''
select 
count(distinct(user_id)) as call_user_num,
count(distinct(user_id * `order`.is_finished)) - if(sum(if(user_id * `order`.is_finished = 0, 1, 0)) > 0, 1, 0) as finished_user_num,
count(distinct(user_id * `order`.is_finished * `user`.is_new)) - if(sum(if(user_id * `order`.is_finished * `user`.is_new = 0, 1, 0)) > 0, 1, 0) as new_finished_user_num
from
(
  SELECT user_id, driver_id, if(status=5,1,0) as is_finished
  FROM oride_db.data_order 
  where dt = "{dt}"
  and
  create_time BETWEEN unix_timestamp('{dt} 00:00:00') and unix_timestamp('{dt} 23:59:59')
) as `order`
join
(
    SELECT id,
    if(register_time BETWEEN unix_timestamp('{dt} 00:00:00') and unix_timestamp('{dt} 23:59:59'),1,0) as is_new
    FROM oride_db.data_user_extend
    where dt = "{dt}"
    and register_time <= unix_timestamp('{dt} 23:59:59')
) as `user`
on `order`.user_id = `user`.id
'''

query5 = '''
SELECT 
count(*),
sum(if(login_time BETWEEN unix_timestamp('{dt} 00:00:00') and unix_timestamp('{dt} 23:59:59'),1,0)) as login_num,
sum(if(register_time BETWEEN unix_timestamp('{dt} 00:00:00') and unix_timestamp('{dt} 23:59:59'),1,0)) as new_num
FROM oride_db.data_driver_extend
where dt = "{dt}"
and register_time <= unix_timestamp('{dt} 23:59:59')
'''

query6 = '''
select
count(distinct(driver_id)) as order_driver_num,
count(distinct(driver_id * `order`.is_finished)) - if(sum(if(driver_id * `order`.is_finished = 0, 1, 0)) > 0, 1, 0) as finished_driver_num,
count(distinct(driver_id * `order`.is_finished * `driver`.is_new)) - if(sum(if(driver_id * `order`.is_finished * `driver`.is_new = 0, 1, 0)) > 0, 1, 0) as new_finished_driver_num
from
(
  SELECT user_id, driver_id, if(status=5,1,0) as is_finished
  FROM oride_db.data_order 
  where dt = "{dt}"
  and
  create_time BETWEEN unix_timestamp('{dt} 00:00:00') and unix_timestamp('{dt} 23:59:59')
) as `order`
join
(
    SELECT id,
    if(register_time BETWEEN unix_timestamp('{dt} 00:00:00') and unix_timestamp('{dt} 23:59:59'),1,0) as is_new
    FROM oride_db.data_driver_extend
    where dt = "{dt}"
    and register_time <= unix_timestamp('{dt} 23:59:59')
) as driver
on `order`.driver_id = driver.id
'''

query7 = '''
select sum(if(action="bubble",1,0)) from oride_source.user_action where dt="{dt}"
'''
# online_driver_num
query8 = '''
select count(distinct driverid) as online_num from (
select driverid, action from oride_source.driver_action where dt="{dt}" and action in
 ("taxi_accept", "login_success", "outset_show",
 "pay_review", "pay_successful", "review_consummation")) as tmp
'''

query9 = '''
SELECT 
sum(if(register_time BETWEEN unix_timestamp('{dt} 00:00:00') and unix_timestamp('{dt} 23:59:59'),1,0)) as new_num
FROM oride_db.data_user_extend
where dt = "{dt}"
and register_time <= unix_timestamp('{dt} 23:59:59')
'''

query_online_drivers = '''
select distinct driverid as online_num from (
select driverid, action from oride_source.driver_action where dt="{dt}" and action in
 ("taxi_accept", "login_success", "outset_show",
 "pay_review", "pay_successful", "review_consummation")) as tmp
'''

# the time should be count after the join of drivers
query_order_time = '''
select take_time, wait_time, pickup_time, arrive_time, finish_time, cancel_time
from oride_db.data_order where dt="{dt}" and 
create_time BETWEEN unix_timestamp('{dt} 00:00:00') and unix_timestamp('{dt} 23:59:59')
'''


INSERT_SQL = '''
REPLACE INTO oride_data.daily_report VALUES (
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
%s, %s, %s, %s, %s, %s, %s)
'''

QUERY_DATA_RANGE = 8
QUERY_EMAIL_DATA = '''
select * from oride_data.daily_report where dt>="%s" and dt<="%s"
'''
col_meaning = ["Date", 'No. of completed ride', 'Fullfillment rate', 'No. of view', 'No. of request',
    'View to request transfer rate', 'Online rider', 'Accepted order rider', 'GMV', 'ASP', 'B-subsidy',
    'C-subsidy', 'Avg. B-sub per order', 'Avg. C-sub per order', 'Total subsidy ratio',
    'Cancel rate before rider accept', 'Cancel rate after rider accept',
    'Driver cancel rate (after driver accept)', 'ATA(min)', 'Avg order accept time(s)',
    'Total registered rider', 'New registered rider', 'Completed-order rider', 'New completed-order rider',
    'New completed-order rider ratio', 'No. of requested passenager', 'Completed order passenager',
    'New registered passenger', 'New completed order passenger',
    'New completed order passenger / Completed order passenager', 'New completed order passenger / New registered passenger',
    'No. of online order', 'No. of offine order',
    # 'Driver online with order time / Driver online time',
    # 'Driver online with order time / Driver work duration (15hours)',
    # 'Avg. Order per online driver'
    ]
not_show_indexs = [col_meaning.index("No. of view"), col_meaning.index("View to request transfer rate")]


def query_repair_table(sql):
    cursor = get_hive_cursor()
    cursor.execute(sql)
    cursor.close()


def query_hive_data(sql):
    cursor = get_hive_cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    return result


def mapper(x):
    if x is None:
        x = 0
    return x


def n_days_ago(n_time, days):
    now_time = datetime.strptime(n_time, '%Y-%m-%d')
    delta = timedelta(days=days)
    n_days = now_time - delta
    return n_days.strftime("%Y-%m-%d")


def raw_data_mapper(x):
    res = 0
    try:
        res = int(x)
    except:
        pass
    return res


def get_driver_online_time(dt):
    reform_dt = "".join(dt.split("-"))
    pika = get_pika_connection()
    pipe = pika.pipeline(transaction=False)
    cursor = get_hive_cursor()
    cursor.execute(query_online_drivers.format(dt=dt))
    res_drivers = cursor.fetchall()
    online_time = 0
    counter = 0
    for driver_id in res_drivers:
        try:
            pipe.get(driver_online_time_key.format(driver_id=driver_id[0], dt=reform_dt))
            counter += 1
            if counter % 128 == 0:
                tmp_res = pipe.execute()
                tmp_res = map(raw_data_mapper, tmp_res)
                online_time += sum(tmp_res)
        except:
            pass
    tmp_res = pipe.execute()
    tmp_res = map(raw_data_mapper, tmp_res)
    online_time += sum(tmp_res)
    return online_time


def get_order_time(dt):
    cursor = get_hive_cursor()
    cursor.execute(query_order_time.format(dt=dt))
    res_order_time = cursor.fetchall()
    driver_take_order_num = 0
    order_time = 0
    for line in res_order_time:
        (take_time, wait_time, pickup_time, arrive_time, finish_time, _) = line
        if take_time > 0:
            driver_take_order_num += 1
            single_order_max_time = max(wait_time, pickup_time, arrive_time, finish_time)
            if single_order_max_time > take_time and single_order_max_time - take_time <= 86400:
                order_time += (single_order_max_time - take_time)
    return driver_take_order_num, order_time


def query_data(**op_kwargs):
    dt = op_kwargs.get('ds')
    cursor = get_hive_cursor()
    cursor.execute("set hive.execution.engine=tez")
    repair_table_names = ["data_driver_extend", "data_driver_reward",
                          "data_order", "data_order_payment", "data_user_extend",
                          "user_action", "driver_action"]
    for name in repair_table_names:
        print(name)
        db_name = "oride_source."
        if name.startswith("data"):
            db_name = "oride_db."
        cursor.execute(repair_table_query % (db_name + name))
    cursor.execute(query1.format(dt=dt))
    res1 = cursor.fetchall()
    res1 = map(mapper, list(res1[0]))
    [call_num, success_num, gmv, cancel_before_dispatching_num, cancel_after_dispatching_by_user_num,
     cancel_after_dispatching_by_driver_num, pickup_num, pickup_total_time, take_num,
     take_total_time] = res1
    print(1)
    cursor.execute(query2.format(dt=dt))
    res2 = cursor.fetchall()
    res2 = map(mapper, list(res2[0]))
    [pay_num, total_price, total_c_discount, offline_num] = res2
    print(2)
    cursor.execute(query3.format(dt=dt))
    res3 = cursor.fetchall()
    res3 = map(mapper, list(res3[0]))
    [order_num, total_driver_price] = res3
    print(3)
    cursor.execute(query4.format(dt=dt))
    res4 = cursor.fetchall()
    res4 = map(mapper, list(res4[0]))
    [call_user_num, finished_user_num, new_finished_user_num] = res4
    print(4)
    cursor.execute(query5.format(dt=dt))
    res5 = cursor.fetchall()
    res5 = map(mapper, list(res5[0]))
    [total_driver_num, login_driver_num, new_driver_num] = res5
    print(5)
    cursor.execute(query6.format(dt=dt))
    res6 = cursor.fetchall()
    res6 = map(mapper, list(res6[0]))
    [order_driver_num, finished_driver_num, new_finished_driver_num] = res6
    print(6)
    cursor.execute(query7.format(dt=dt))
    res7 = cursor.fetchall()
    res7 = map(mapper, list(res7[0]))
    [bubble_num] = res7
    print(7)
    cursor.execute(query8.format(dt=dt))
    res8 = cursor.fetchall()
    res8 = map(mapper, list(res8[0]))
    [online_driver_num] = res8
    print(8)
    cursor.execute(query9.format(dt=dt))
    res9 = cursor.fetchall()
    res9 = map(mapper, list(res9[0]))
    [new_passenger_num] = res9
    print(9)
    driver_time = get_driver_online_time(dt)
    print(10)
    driver_take_order_num, order_time = get_order_time(dt)
    print(11)
    transport_efficiency = order_time / float(driver_time) if driver_time > 0 else 0
    enthusiasm = order_time / float(work_times * online_driver_num) if online_driver_num > 0 else 0
    order_num_per_driver = driver_take_order_num / float(online_driver_num) if online_driver_num > 0 else 0
    data = [
        success_num,
        success_num / float(call_num) if call_num > 0 else 0,
        bubble_num,
        call_num,
        call_num / float(bubble_num) if bubble_num > 0 else 0,
        online_driver_num,
        order_driver_num,
        round(float(gmv), 2),
        round(float(gmv) / float(success_num) if success_num > 0 else 0, 2),
        round(float(total_driver_price), 2),
        round(float(total_c_discount), 2),
        round(float(total_driver_price) / float(success_num) if success_num > 0 else 0, 2),
        round(float(total_c_discount) / float(success_num) if success_num > 0 else 0, 2),
        float(total_driver_price + total_c_discount) / float(total_price) if total_price > 0 else 0,
        cancel_before_dispatching_num / float(call_num) if call_num > 0 else 0,
        cancel_after_dispatching_by_user_num / float(call_num) if call_num > 0 else 0,
        cancel_after_dispatching_by_driver_num / float(call_num) if call_num > 0 else 0,
        round(pickup_total_time / float(pickup_num * 60) if pickup_num > 0 else 0, 2),
        round(take_total_time / float(take_num) if take_num > 0 else 0, 2),
        total_driver_num,
        new_driver_num,
        finished_driver_num,
        new_finished_driver_num,
        new_finished_driver_num / float(finished_driver_num) if finished_driver_num > 0 else 0,
        call_user_num,
        finished_user_num,
        new_passenger_num,
        new_finished_user_num,
        new_finished_user_num / float(finished_user_num) if finished_driver_num > 0 else 0,
        new_finished_user_num / new_passenger_num if new_passenger_num > 0 else 0,
        pay_num - offline_num,
        offline_num,
        transport_efficiency,
        enthusiasm,
        order_num_per_driver
    ]
    insert_data = [None, dt] + data
    sql_conn = get_db_conn()
    sql_cursor = sql_conn.cursor()
    sql_cursor.execute(INSERT_SQL, insert_data)


def write_email(**op_kwargs):
    dt = op_kwargs.get('ds')
    init_day = n_days_ago(dt, QUERY_DATA_RANGE)
    sql_conn = get_db_conn()
    sql_cursor = sql_conn.cursor()
    sql_cursor.execute(QUERY_EMAIL_DATA % (init_day, dt))
    res = sql_cursor.fetchall()
    res = list(res)
    if len(res) < 1:
        return
    res = map(list, res)
    arr = []
    for elem in res:
        elem[1] = elem[1].strftime('%Y-%m-%d')
        arr.append(elem)
    arr.sort(key=lambda x: x[1], reverse=True)
    h = mail_msg_header.format(dt1=arr[0][1], dt2=arr[-1][1])
    for x in range(len(col_meaning)):
        if x in not_show_indexs:
            continue
        h += part_html1.format(key=col_meaning[x])
        for y in range(len(arr)):
            tmp_val = arr[y][x + 1]
            if "ratio" in col_meaning[x] or "rate" in col_meaning[x] or "/" in col_meaning[x]:
                tmp_val = "%.2f%%" % (tmp_val * 100)
            h += part_html2.format(val=tmp_val) if x > 0 else part_html2_1.format(val=tmp_val)
        h += part_html3
    h += mail_msg_tail
    h += css_style
    message = MIMEText(h, 'html', 'utf-8')
    subject = 'Oride {dt1} -- {dt2} Daily Report'.format(dt1=arr[0][1], dt2=arr[-1][1])
    message['Subject'] = Header(subject, 'utf-8')
    try:
        server = smtplib.SMTP('mail.opay-inc.com', 25)
        server.ehlo()
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, receivers, message.as_string())
        print("邮件发送成功")
    except smtplib.SMTPException as e:
        print(e.message)

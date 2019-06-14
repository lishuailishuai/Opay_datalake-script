# -*- coding: utf-8 -*-
from utils.connection_helper import get_hive_cursor
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import csv
from email.mime.multipart import MIMEMultipart
bad_debt_email_limit = 50
sender = 'research@opay-inc.com'
password = 'G%4nlD$YCJol@Op'
# receivers = ['lichang.zhang@opay-inc.com']
receivers = ['lichang.zhang@opay-inc.com', 'tianzhi.zhang@opay-inc.com',
             'lei.zheng@opay-inc.com', 'dinglun.fan@opay-inc.com', 'haihuan.zou@opay-inc.com']

part_html1 = """<tr>"""
part_html2 = """<td class="value_td">{val}</td>"""
part_html3 = """</tr>"""

css_style = '''
<style type="text/css">
.title_td{border:solid#000 1px;width:300px;overflow:hidden;text-align:center}
.value_td{border:solid#000 1px;width:100px;overflow:hidden;text-align:right}
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
"""

table_head = '''
    <table class="table" cellpadding="5">
      <caption><h4>Oride {dt} {view} Bad Debts Daily Report</h4></caption>
'''
simply_table_head = '''
    <table class="table" cellpadding="5">
'''
table_tail = '''
    </table>
'''

mail_msg_tail = '''
  </body>
</html>
'''

repair_table_query = '''
MSCK REPAIR TABLE oride_db.%s
'''

query1 = '''
select a.id as id, a.user_id as user_id, a.driver_id as driver_id, a.price as price
from
(select 
id,
user_id,
driver_id,
price
from oride_db.data_order
where dt = "{dt}" and status = 4)a
left join
(select 
id,
case 
when mode='0' then '未知'
when mode='1' then '线下支付'
when mode='2' then 'opay'
when mode='3' then '余额'
end as mode,
price,
case when status='0' then '支付中'
when status='1' then '成功'
when status='2' then '失败'
end as zfstatus
from oride_db.data_order_payment
where dt="{dt}")as b
on a.id=b.id												
'''

tmp_query = """
select 
a.id,
a.user_id,
a.start_name,
a.end_name,
a.duration,
a.distance,
a.price,
a.reward,
a.driver_id,
a.take_time,
a.wait_time,
a.pickup_time,
a.arrive_time,
a.finish_time,
a.cancel_role,
a.cancel_reason,
a.cancel_time,
a.cancel_type,
a.status,
a.dt,
b.id,
b.driver_id,
b.mode,
b.price,
b.coupon_id,
b.coupon_amount,
b.amount,
b.bonus,
b.balance,
b.opay_amount,
b.reference,
b.currency,
b.country,
b.zfstatus,
b.modify_time,
b.create_time,
b.dt 
from
(select 
id,
user_id,
start_name,
end_name,
duration,
distance,
price,
reward,
driver_id,
from_unixtime(take_time) as take_time,
from_unixtime(wait_time) as wait_time,
from_unixtime(pickup_time)as pickup_time,
from_unixtime(arrive_time)as arrive_time,
from_unixtime(finish_time)as finish_time,
cancel_role,
cancel_reason,
from_unixtime(cancel_time) as cancel_time,
cancel_type,
status,
dt
from oride_db.data_order
where dt ="{dt}" and take_time between unix_timestamp('{dt} 00:00:00')and unix_timestamp('{dt} 23:59:59')and status='4')a
left join 
(select 
id,
driver_id,
case 
when mode='0'then '未知'
when mode='1'then '线下支付'
when mode='2'then 'opay'
when mode='3'then '余额'
end as mode,
price,
coupon_id,
coupon_amount,
amount,
bonus,
balance,
opay_amount,
reference,
currency,
country,
case when status='0' then '支付中'
when status='1' then '成功'
when status='2' then '失败'
end as zfstatus,
modify_time,
from_unixtime(create_time) as create_time,
dt 
from oride_db.data_order_payment
where dt="{dt}" and status<>'1')as b
on a.id=b.id
"""


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


def build_table(data, t_head):
    for elem in data:
        for x in elem:
            t_head += part_html2.format(val=x)
        t_head += part_html3
    t_head += table_tail
    return t_head


def build_csv(dt):
    col_name = ['a.id', 'a.user_id', 'a.start_name', 'a.end_name', 'a.duration', 'a.distance', 'a.price', 'a.reward', 'a.driver_id', 'a.take_time', 'a.wait_time', 'a.pickup_time', 'a.arrive_time', 'a.finish_time', 'a.cancel_role', 'a.cancel_reason', 'a.cancel_time', 'a.cancel_type', 'a.status', 'a.dt', 'b.id', 'b.driver_id', 'b.mode', 'b.price', 'b.coupon_id', 'b.coupon_amount', 'b.amount', 'b.bonus', 'b.balance', 'b.opay_amount', 'b.reference', 'b.currency', 'b.country', 'b.zfstatus', 'b.modify_time', 'b.create_time', 'b.dt']
    cursor = get_hive_cursor()
    cursor.execute(tmp_query.format(dt=dt))
    res = cursor.fetchall()
    with open("/tmp/tainzhi_query_%s.csv" % dt, "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(col_name)
        for elem in res:
            csv_writer.writerow(elem)
    print("tz csv write done")


def build_html_txt(data_user, data_driver, dt):
    h = mail_msg_header
    amount_of_order = sum([x[1] for x in data_user[1:]])
    amount_of_price = sum([x[2] for x in data_user[1:]])
    t_money_sum_data = [
        ['No. order', 'No. price', 'No. related users', 'No. related drivers'],
        [amount_of_order, amount_of_price, len(data_user), len(data_driver)]
    ]
    t_money_html = build_table(t_money_sum_data, simply_table_head)
    driver_num = len(data_driver[1:])
    t_driver_stats_data = [
        ['No. driver', 'rate of 1 order/driver', 'rate of 2 order/driver', 'rate of 3 order/driver', 'more'],
        [driver_num,
         "%.2f%%" % (len([x for x in data_driver[1:] if x[1] == 1]) / float(driver_num) * 100) if driver_num > 0 else 0,
         "%.2f%%" % (len([x for x in data_driver[1:] if x[1] == 2]) / float(driver_num) * 100) if driver_num > 0 else 0,
         "%.2f%%" % (len([x for x in data_driver[1:] if x[1] == 3]) / float(driver_num) * 100) if driver_num > 0 else 0,
         "%.2f%%" % (len([x for x in data_driver[1:] if x[1] > 3]) / float(driver_num) * 100) if driver_num > 0 else 0]
    ]
    t_driver_stat_html = build_table(t_driver_stats_data, simply_table_head)
    t1 = build_table(data_user, table_head.format(dt=dt, view="User"))
    t2 = build_table(data_driver, table_head.format(dt=dt, view="Driver"))
    h += t_money_html
    h += t_driver_stat_html
    h += t1
    h += t2
    h += mail_msg_tail
    h += css_style
    message = MIMEMultipart()
    message.attach(MIMEText(h, 'html', 'utf-8'))
    subject = 'Oride {dt} Bad Debts Daily Report'.format(dt=dt)
    message['Subject'] = Header(subject, 'utf-8')
    att1 = MIMEText(open("/tmp/tainzhi_query_%s.csv" % dt, 'r').read(), 'plain', 'utf-8')
    att1["Content-Type"] = 'application/octet-stream'
    att1["Content-Disposition"] = 'attachment; filename="tianzhi_query_%s.csv"' % dt
    message.attach(att1)
    return message


def check_bad_debts_data(**op_kwargs):
    dt = op_kwargs.get('ds')
    print(dt)
    cursor = get_hive_cursor()
    cursor.execute("set hive.execution.engine=tez")
    repair_table_names = ["data_order", "data_order_payment"]
    for name in repair_table_names:
        print(name)
        cursor.execute(repair_table_query % name)
    build_csv(dt)
    cursor.execute(query1.format(dt=dt))
    res1 = cursor.fetchall()
    user_view_bad_debts = {}
    driver_view_bad_debts = {}
    for line in res1:
        (order_id, user_id, driver_id, price) = line
        price = float(price)
        if user_id not in user_view_bad_debts:
            user_view_bad_debts[user_id] = [set(), 0]
        if order_id not in user_view_bad_debts[user_id][0]:
            user_view_bad_debts[user_id][0].add(order_id)
            user_view_bad_debts[user_id][1] += price
        if driver_id not in driver_view_bad_debts:
            driver_view_bad_debts[driver_id] = [set(), 0]
        if order_id not in driver_view_bad_debts[driver_id][0]:
            driver_view_bad_debts[driver_id][0].add(order_id)
            driver_view_bad_debts[driver_id][1] += price
    user_data, driver_data = [], []
    for uid in user_view_bad_debts:
        user_data.append([uid, len(user_view_bad_debts[uid][0]), user_view_bad_debts[uid][1]])
    for did in driver_view_bad_debts:
        driver_data.append([did, len(driver_view_bad_debts[did][0]), driver_view_bad_debts[did][1]])
    # sort according to the amount price
    user_data.sort(key=lambda x: x[2], reverse=True)
    driver_data.sort(key=lambda x: x[2], reverse=True)
    user_titles = ["user_id", "amount of order", "amount of price"]
    driver_titles = ["driver_id", "amount of order", "amount of price"]
    user_data = [user_titles] + user_data[:bad_debt_email_limit]
    driver_data = [driver_titles] + driver_data[:bad_debt_email_limit]
    msg = build_html_txt(user_data, driver_data, dt)
    try:
        server = smtplib.SMTP('mail.opay-inc.com', 25)
        server.ehlo()
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, receivers, msg.as_string())
        print("邮件发送成功")
    except smtplib.SMTPException as e:
        print(e.message)

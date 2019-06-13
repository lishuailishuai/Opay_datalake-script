from utils.connection_helper import get_db_conn, get_hive_cursor
import requests
import time
from datetime import datetime, timedelta
import json

test_url = "http://dev.api.o-pay.in/admin/sendpush"
prod_url = "http://api.o-pay.in/admin/sendpush"

title1 = "You have an uncompleted order"
body1 = "You have an order that hasn’t been paid for, please login on the app to complete payment."

title2 = "Reward Interception"
body2 = "You have a reward which has been intercepted by our control department, it will be deducted during your next payment. For questions, please contact the customer service."

driver_action = "oride://ride.driver/moneyManager"
user_action = "ride://ride.passenger"

not_pay_hql = '''
select id from oride_db.{table_name} where status = 4 and dt = '{dt}'
and user_id not in (
select distinct(user_id) from oride_db.{table_name2}
where dt = '{dt}' and is_white = 1
)
'''
not_pay_sql = '''
select distinct(user_id) from data_order where status = 4 and id in ({ids})
'''

abnormal_sql = """
select distinct(a1.driver_id) from
(select order_id, driver_id from oride_db.{table_record}
where dt = '{dt}' and amount < 0
and created_at >= unix_timestamp('{dt} 00:00:00')
and driver_id not in (
select distinct(driver_id) as did from oride_db.{table_white} where dt = '{dt}' and is_white=1)
) a1
join
(select order_id, driver_id from oride_db.{table_abnormal}
where dt = '{dt}' and is_revoked = 0) a2
on a1.order_id = a2.order_id
"""


def get_lagos_timestamp(dt):
    t = time.strptime(dt, '%Y-%m-%d')
    y, m, d = t[0:3]
    date = datetime(year=y, month=m, day=d, hour=9) + timedelta(days=1)
    timeStamp = int(time.mktime(date.timetuple()))
    return timeStamp


def send_push(env, role, role_id, lagos_time, push_type=""):
    cur_timestamp = int(time.time())
    time_gap = lagos_time - cur_timestamp
    if push_type == "deduct":
        title = title2
        body = body2
        action = driver_action
    elif push_type == "not_pay":
        title = title1
        body = body1
        action = user_action
    else:
        return
    url = test_url
    if env != "test":
        url = prod_url
    params = {
        "role": role,
        "role_id": role_id,
        "title": title,
        "action": action,
        "body": body,
        "delay": time_gap if time_gap > 0 else 0,
    }
    res = requests.post(url, json.dumps(params), timeout=1)
    print(res.status_code)


def not_pay_push(**op_kwargs):
    dt = op_kwargs.get('ds')
    env = op_kwargs.get('env', 'prod')
    lagos_9_clock_timestamp = get_lagos_timestamp(dt)
    cursor = get_hive_cursor()
    table_name = 'data_order'
    table_name2 = 'data_user_whitelist'
    if env == 'test':
        table_name += '_dev'
        table_name2 += '_dev'
    cursor.execute("msck repair table oride_db.%s" % table_name)
    cursor.execute("msck repair table oride_db.%s" % table_name2)
    cursor.execute(not_pay_hql.format(table_name=table_name, table_name2=table_name2, dt=dt))
    res = [x[0] for x in cursor.fetchall()]
    print("not pay order ids: %d" % len(res))
    step = 100
    db_name = 'sqoop_db'
    if env == 'test':
        db_name += '_test'
    mysql_cursor = get_db_conn(db_name).cursor()
    uids = set()
    for i in range(0, len(res), step):
        tmp = [str(x) for x in res[i:i + step]]
        sql = not_pay_sql.format(ids=','.join(tmp))
        mysql_cursor.execute(sql)
        data = mysql_cursor.fetchall()
        for rec in data:
            uids.add(rec[0])
    print("not pay user ids: %d" % len(uids))
    for uid in uids:
        send_push(env, 1, uid, lagos_9_clock_timestamp, "not_pay")


def abnormal_push(**op_kwargs):
    dt = op_kwargs.get('ds')
    env = op_kwargs.get('env', 'prod')
    lagos_9_clock_timestamp = get_lagos_timestamp(dt)
    cursor = get_hive_cursor()
    table_record = 'data_driver_recharge_records'
    table_abnormal = 'data_abnormal_order'
    table_white = 'data_driver_whitelist'
    if env == 'test':
        table_record += '_dev'
        table_abnormal += '_dev'
        table_white += '_dev'
    cursor.execute("msck repair table oride_db.%s" % table_record)
    cursor.execute("msck repair table oride_db.%s" % table_abnormal)
    cursor.execute("msck repair table oride_db.%s" % table_white)
    cursor.execute(abnormal_sql.format(table_record=table_record, table_white=table_white, table_abnormal=table_abnormal,dt=dt))
    abnormal_drivers = [x[0] for x in cursor.fetchall()]
    print("abnormal order related drivers: %d" % len(abnormal_drivers))
    for did in abnormal_drivers:
        send_push(env, 2, did, lagos_9_clock_timestamp, "deduct")

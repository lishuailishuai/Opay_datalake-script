# -*- coding: utf-8 -*-
from utils.connection_helper import get_hive_cursor, get_redis_connection

driver_online_time_key = "driver:ont:%d:%s"
driver_order_time_key = "driver:ort:%d:%s"

sql1 = """
select distinct(common.user_id) from oride_source.client_event 
where dt = '{dt}' and 
`timestamp` BETWEEN unix_timestamp('{dt} 00:00:00') and unix_timestamp('{dt} 23:59:59')
"""

sql2 = '''
select substr(from_unixtime(`timestamp`), 12, 2) as `hour`, count(distinct(common.user_id)) as driver_num
from oride_source.client_event
where dt = '{dt}'
group by substr(from_unixtime(`timestamp`), 12, 2)
'''

sql3 = '''
select 
a.driver_id,
sum(if(status in (4, 5),a.distance,0)) as distance,
sum(if(status in (4, 5),a.price,0)) as money,
count(*) as order_num,
sum(if(status in (4, 5),1,0)) as arrived_order_num,
sum(if(b.score is not null, 1, 0)) as score_num,
sum(if(b.score is not null, b.score, 0)) as score_sum
from
(select * from oride_db.data_order
where dt = '{dt}'
and create_time between unix_timestamp('{dt} 00:00:00') and unix_timestamp('{dt} 23:59:59')
) a
left outer join
(
select * from oride_db.data_driver_comment
where dt = '{dt}'
) b
on a.id = b.order_id
group by a.driver_id
'''


def get_driver_data(dt):
    cursor = get_hive_cursor()
    cursor.execute("set hive.execution.engine=tez")
    cursor.execute("msck repair table oride_source.client_event")
    cursor.execute("msck repair table oride_db.data_order")
    driver_stats = {}
    cursor.execute(sql2.format(dt=dt))
    res = cursor.fetchall()
    for x in res:
        driver_stats[x[0]] = x[1]
    cursor.execute(sql1.format(dt=dt))
    res = cursor.fetchall()
    driver_data = {}
    for x in res:
        if len(x) > 0 and x[0].isdigit():
            if int(x[0]) != 0:
                driver_data[int(x[0])] = [0] * 8
    online_driver_num = len(driver_data)
    cursor.execute(sql3.format(dt=dt))
    res = cursor.fetchall()
    for x in res:
        if x[0] in driver_data:
            for i in range(1, 7):
                driver_data[x[0]][i - 1] = x[i]
    cache = get_redis_connection()
    pipe = cache.pipeline(transaction=False)
    nodash_dt = "".join(dt.split("-"))
    driver_ids = []
    for driver_id in driver_data:
        driver_ids.append(driver_id)
        _ = pipe.get(driver_online_time_key % (driver_id, nodash_dt))
        _ = pipe.get(driver_order_time_key % (driver_id, nodash_dt))
    tmp_res = pipe.execute()
    for i in range(len(driver_ids)):
        driver_data[driver_ids[i]][6] = 0 if tmp_res[i * 2] is None else int(tmp_res[i * 2])
        driver_data[driver_ids[i]][7] = 0 if tmp_res[i * 2 + 1] is None else int(tmp_res[i * 2 + 1])
    with open("/tmp/%s_online_driver_num.csv" % dt, "w") as fout:
        fout.write("hour,driver_num\n")
        fout.write("all,%d\n" % len(driver_data))
        for i in range(24):
            x = "%02d" % i
            y = 0 if x not in driver_stats else driver_stats[x]
            fout.write("%s,%s\n" % (x, y))
    order_num, eff = 0, 0
    with open("/tmp/%s_driver_data.csv" % dt, "w") as fout:
        fout.write("driver_id,total_distance,total_income,order_num,arrived_order_num,scored_order_num,avg_score,online_time,order_time,order_time/online_time\n")
        for driver_id in driver_data:
            fout.write(str(driver_id))
            rec = driver_data[driver_id]
            rec[5] = 0 if rec[4] == 0 else ("%.2f" % (rec[5] / rec[4]))
            for i in range(8):
                fout.write(",%s" % rec[i])
            v = rec[7] * 100.0 / rec[6] if rec[6] > 0 else 0
            order_num += rec[3]
            eff += v / 100
            fout.write(",%.2f%%" % v)
            fout.write("\n")
    if len(driver_data) > 0:
        order_num /= len(driver_data)
        eff /= len(driver_data)
    return eff, order_num, online_driver_num
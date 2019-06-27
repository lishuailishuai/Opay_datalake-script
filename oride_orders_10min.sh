#!/usr/bin/env bash

HOST_RD=$1
PORT_RD=$2
USER_RD=$3
PASS_RD=$4
HOST_BI=$5
PORT_BI=$6
USER_BI=$7
PASS_BI=$8

mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e"
    insert ignore into oride_orders_status_hourly
        (daily, hourly)
    values (date_format(from_unixtime(unix_timestamp()-3600), '%Y-%m-%d'), date_format(from_unixtime(unix_timestamp()-3600), '%k')),
        (date_format(now(), '%Y-%m-%d'), date_format(now(), '%k'))
"

while read day hour orders orders_user orders_pick; do
    mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e"
        insert into oride_orders_status_hourly
            (daily, hourly, orders, orders_user, orders_pick)
        values ('${day}', ${hour}, ${orders}, ${orders_user}, ${orders_pick})
            on duplicate key update orders=values(orders), orders_user=values(orders_user), orders_pick=values(orders_pick)"
done <<_eof
    $(mysql -h${HOST_RD} -u${USER_RD} -P${PORT_RD} -p${PASS_RD} oride_data --skip-column-names -e"set time_zone = '+1:00'; select date_format(from_unixtime(create_time), '%Y-%m-%d') as order_time, date_format(from_unixtime(create_time), '%k') as order_hour, count(1), count(distinct user_id), sum(if(driver_id>0,1,0)) from oride_data.data_order where create_time>=unix_timestamp(date_format(now(),'%Y-%m-%d %H'))-3600 group by order_time, order_hour")
_eof

while read day hour drivers; do
    mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e" insert into oride_orders_status_hourly
        (daily, hourly, drivers_serv)
    values ('${day}', ${hour}, ${drivers})
    on duplicate key update drivers_serv=values(drivers_serv)"
done<<_feof
    $(mysql  -h${HOST_RD} -u${USER_RD} -P${PORT_RD} -p${PASS_RD} oride_data --skip-column-names -e"set time_zone = '+1:00'; select date_format(now(), '%Y-%m-%d'), date_format(now(), '%k'), count(1) from data_driver_extend where serv_mode>0")
_feof
#!/usr/bin/env bash

HOST_RD=$1
PORT_RD=$2
USER_RD=$3
PASS_RD=$4
HOST_BI=$5
PORT_BI=$6
USER_BI=$7
PASS_BI=$8

CURR_TIMESTAMP=$(date +'%s')
PREV_10MIN=$((CURR_TIMESTAMP - 600))
PREV_1HOUR=$(date +'%Y-%m-%d %H:00:00' --date="1 hour ago")
PREV_HOUR_TIMESTAMP=$(date +'%s' --date="${PREV_1HOUR}")

MYSQL_VALUES=""
while ((${PREV_HOUR_TIMESTAMP} <= ${PREV_10MIN})); do
    DAY_10MIN=$(date +'%Y-%m-%d %H:%M:00' --date=@${PREV_HOUR_TIMESTAMP})
    DAY_DAY=$(date +'%Y-%m-%d 00:00:00' --date=@${PREV_HOUR_TIMESTAMP})
    if [ -z "${MYSQL_VALUES}" ];then
        MYSQL_VALUES="('${DAY_10MIN}', '${DAY_DAY}')"
    else
        MYSQL_VALUES="${MYSQL_VALUES},('${DAY_10MIN}', '${DAY_DAY}')"
    fi
    PREV_HOUR_TIMESTAMP=$((PREV_HOUR_TIMESTAMP + 600))
done

if [ -n "${MYSQL_VALUES}" ];then
    mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e"
        insert ignore into oride_orders_status_10min (order_time, daily) values ${MYSQL_VALUES}
    "
fi

#下单量、下单人数、接单量、完单量、平均应答时长(s)、平均接驾时长(m)
while read daytime1 daytime2 day1 day2 orders orders_user orders_pick orders_finish avg_pick avg_take; do
    mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e"
        insert into oride_orders_status_10min
            (order_time, daily, orders, orders_user, orders_pick, orders_finish, avg_pick, avg_take)
        values
            ('${daytime1} ${daytime2}', '${day1} ${day2}', ${orders}, ${orders_user}, ${orders_pick}, ${orders_finish}, ${avg_pick}, ${avg_take})
        on duplicate key update
            orders=values(orders),
            orders_user=values(orders_user),
            orders_pick=values(orders_pick),
            orders_finish=values(orders_finish),
            avg_pick=values(avg_pick),
            avg_take=values(avg_take)"
done <<_eof
    $(mysql -h${HOST_RD} -u${USER_RD} -P${PORT_RD} -p${PASS_RD} oride_data --skip-column-names -e"set time_zone = '+1:00'; select date_format(from_unixtime(floor(create_time/600)*600), '%Y-%m-%d %H:%i:00') as order_time, date_format(from_unixtime(create_time), '%Y-%m-%d 00:00:00') as order_day, count(1) as orders, count(distinct user_id) as orders_user, sum(if(driver_id>0 and take_time>0,1,0)) as orders_pick, sum(if(status=4 or status=5,1,0)) as orders_finish, if(sum(if(driver_id>0 and take_time>0,1,0))>0, floor(sum(if(driver_id>0 and take_time>0, take_time-create_time, 0))/sum(if(driver_id>0 and take_time>0,1,0))), 0) as avg_pick, if(sum(if(status=4 or status=5,1,0))>0, round(sum(if(pickup_time>0, pickup_time-take_time, 0))/60/sum(if(status=4 or status=5,1,0)), 1), 0) as avg_take from oride_data.data_order where create_time>=unix_timestamp(date_format(now(),'%Y-%m-%d %H'))-7200 and create_time<floor(unix_timestamp()/600)*600 group by order_time, order_day")
_eof

#在线司机数瞬时值
#while read daytime1 daytime2 day1 day2 drivers; do
#    mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e" insert into oride_orders_status_10min
#        (order_time, daily, drivers_serv)
#    values ('${daytime1} ${daytime2}', '${day1} ${day2}', ${drivers})
#    on duplicate key update drivers_serv=values(drivers_serv)"
#done<<_feof
#    $(mysql  -h${HOST_RD} -u${USER_RD} -P${PORT_RD} -p${PASS_RD} oride_data --skip-column-names -e"set time_zone = '+1:00'; select date_format(from_unixtime(floor((unix_timestamp()-600)/600)*600), '%Y-%m-%d %H:%i:00'), date_format(from_unixtime(unix_timestamp()-600), '%Y-%m-%d 00:00:00'), count(1) from data_driver_extend where serv_mode>0")
#_feof
#!/usr/bin/env bash

HOST_RD=$1
PORT_RD=$2
USER_RD=$3
PASS_RD=$4
HOST_BI=$5
PORT_BI=$6
USER_BI=$7
PASS_BI=$8

DATA_TABLE="oride_orders_status_10min"
CURR_TIMESTAMP=$(date +'%s')
PREV_10MIN=$((CURR_TIMESTAMP - 600))
PREV_1HOUR=$(date +'%Y-%m-%d %H:00:00' --date="3 hour ago")

MYSQL_VALUES=""

#补齐所有需要统计的记录点, 城市、类型、时间点、日期
PREV_HOUR_TIMESTAMP=$(date +'%s' --date="${PREV_1HOUR}")
while ((${PREV_HOUR_TIMESTAMP} <= ${PREV_10MIN})); do
    DAY_10MIN=$(date +'%Y-%m-%d %H:%M:00' --date=@${PREV_HOUR_TIMESTAMP})
    DAY_DAY=$(date +'%Y-%m-%d 00:00:00' --date=@${PREV_HOUR_TIMESTAMP})
    if [ -z "${MYSQL_VALUES}" ];then
        MYSQL_VALUES="(0,'all',-1,'${DAY_10MIN}','${DAY_DAY}'),(0,'all',0,'${DAY_10MIN}','${DAY_DAY}'),\
            (0,'all',1,'${DAY_10MIN}','${DAY_DAY}'),(0,'all',2,'${DAY_10MIN}','${DAY_DAY}'),(0,'all',99,'${DAY_10MIN}','${DAY_DAY}')"
    else
        MYSQL_VALUES="${MYSQL_VALUES},(0,'all',-1,'${DAY_10MIN}','${DAY_DAY}'),(0,'all',0,'${DAY_10MIN}','${DAY_DAY}'),\
            (0,'all',1,'${DAY_10MIN}','${DAY_DAY}'),(0,'all',2,'${DAY_10MIN}','${DAY_DAY}'),(0,'all',99,'${DAY_10MIN}','${DAY_DAY}')"
    fi
    PREV_HOUR_TIMESTAMP=$((PREV_HOUR_TIMESTAMP + 600))
done

while read city_id city; do
    PREV_HOUR_TIMESTAMP=$(date +'%s' --date="${PREV_1HOUR}")
    while ((${PREV_HOUR_TIMESTAMP} <= ${PREV_10MIN})); do
        DAY_10MIN=$(date +'%Y-%m-%d %H:%M:00' --date=@${PREV_HOUR_TIMESTAMP})
        DAY_DAY=$(date +'%Y-%m-%d 00:00:00' --date=@${PREV_HOUR_TIMESTAMP})
        if [ -z "${MYSQL_VALUES}" ];then
            MYSQL_VALUES="('${city_id}','${city}',-1,'${DAY_10MIN}','${DAY_DAY}'),('${city_id}','${city}',0,'${DAY_10MIN}','${DAY_DAY}'),\
                ('${city_id}','${city}',1,'${DAY_10MIN}','${DAY_DAY}'),('${city_id}','${city}',2,'${DAY_10MIN}','${DAY_DAY}'),('${city_id}','${city}',99,'${DAY_10MIN}','${DAY_DAY}')"
        else
            MYSQL_VALUES="${MYSQL_VALUES},('${city_id}','${city}',-1,'${DAY_10MIN}','${DAY_DAY}'),('${city_id}','${city}',0,'${DAY_10MIN}','${DAY_DAY}'),\
                ('${city_id}','${city}',1,'${DAY_10MIN}','${DAY_DAY}'),('${city_id}','${city}',2,'${DAY_10MIN}','${DAY_DAY}'),('${city_id}','${city}',99,'${DAY_10MIN}','${DAY_DAY}')"
        fi
        PREV_HOUR_TIMESTAMP=$((PREV_HOUR_TIMESTAMP + 600))
    done
done<<_ceof
$(mysql -h${HOST_RD} -u${USER_RD} -P${PORT_RD} -p${PASS_RD} oride_data --skip-column-names -e"
    select id, name from data_city_conf where id < 999000
")
_ceof

if [ -n "${MYSQL_VALUES}" ];then
    #echo "insert ignore into ${DATA_TABLE} (city_name, serv_type, order_time, daily) values ${MYSQL_VALUES}"
    mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e"
        insert into ${DATA_TABLE} (city_id, city_name, serv_type, order_time, daily) values ${MYSQL_VALUES} on duplicate key update city_name=values(city_name)
    "
fi


#下单量、下单人数、接单量、完单量、平均应答时长(s)、平均接驾时长(m)
#1. 汇总数据
while read type city_id city daytime1 daytime2 day1 day2 orders orders_user orders_pick orders_finish avg_pick avg_take not_sys_cancel_orders picked_orders orders_accept; do
    mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e"
        insert into ${DATA_TABLE}
            (city_id, city_name, serv_type, order_time, daily, orders, orders_user, orders_pick, orders_finish, avg_pick, avg_take,not_sys_cancel_orders,picked_orders,orders_accept)
        values
            ('${city_id}', '${city}', '${type}', '${daytime1} ${daytime2}', '${day1} ${day2}', '${orders}', '${orders_user}', '${orders_pick}', '${orders_finish}', '${avg_pick}', '${avg_take}', '${not_sys_cancel_orders}', '${picked_orders}', '${orders_accept}')
        on duplicate key update
            city_name=values(city_name),
            orders=values(orders),
            orders_user=values(orders_user),
            orders_pick=values(orders_pick),
            orders_finish=values(orders_finish),
            avg_pick=values(avg_pick),
            avg_take=values(avg_take),
            not_sys_cancel_orders=values(not_sys_cancel_orders),
            picked_orders=values(picked_orders),
            orders_accept=values(orders_accept)"
done <<_eof
$(mysql -h${HOST_RD} -u${USER_RD} -P${PORT_RD} -p${PASS_RD} oride_data --skip-column-names -e"
    set time_zone = '+1:00';
    select
        -1,
        0,
        'all',
        date_format(from_unixtime(floor(create_time/600)*600), '%Y-%m-%d %H:%i:00') as order_time,
        date_format(from_unixtime(create_time), '%Y-%m-%d 00:00:00') as order_day,
        count(1) as orders,
        count(distinct user_id) as orders_user,
        sum(if(driver_id>0 and take_time>0,1,0)) as orders_pick,
        sum(if(status=4 or status=5,1,0)) as orders_finish,
        if(sum(if(driver_id>0 and take_time>0,1,0))>0,
            floor(sum(if(driver_id>0 and take_time>0, take_time-create_time, 0))/sum(if(driver_id>0 and take_time>0,1,0))), 0) as avg_pick,
        if(sum(if(pickup_time>0,1,0))>0,
            round(sum(if(pickup_time>0, pickup_time-take_time, 0))/60/sum(if(pickup_time>0,1,0)), 1), 0) as avg_take,
        count(if(status = 6 and driver_id <> 0 and  cancel_role <> 3 and cancel_role <> 4,id,null)) as not_sys_cancel_orders,
        count(if(pickup_time > 0,id,null)) as picked_orders,
        count(if(take_time>0,id,null)) as orders_accept
    from oride_data.data_order
    where create_time>=unix_timestamp(date_format(now(),'%Y-%m-%d %H'))-7200 and
        create_time<floor(unix_timestamp()/600)*600
    group by order_time, order_day
")
_eof

#2. 城市汇总
while read type city_id city daytime1 daytime2 day1 day2 orders orders_user orders_pick orders_finish avg_pick avg_take not_sys_cancel_orders picked_orders orders_accept; do
    mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e"
        insert into ${DATA_TABLE}
            (city_id, city_name, serv_type, order_time, daily, orders, orders_user, orders_pick, orders_finish, avg_pick, avg_take,not_sys_cancel_orders,picked_orders,orders_accept)
        values
            ('${city_id}', '${city}', '${type}', '${daytime1} ${daytime2}', '${day1} ${day2}', '${orders}', '${orders_user}', '${orders_pick}', '${orders_finish}', '${avg_pick}', '${avg_take}', '${not_sys_cancel_orders}', '${picked_orders}', '${orders_accept}')
        on duplicate key update
            city_name=values(city_name),
            orders=values(orders),
            orders_user=values(orders_user),
            orders_pick=values(orders_pick),
            orders_finish=values(orders_finish),
            avg_pick=values(avg_pick),
            avg_take=values(avg_take),
            not_sys_cancel_orders=values(not_sys_cancel_orders),
            picked_orders=values(picked_orders),
            orders_accept=values(orders_accept)"
done <<_eof
$(mysql -h${HOST_RD} -u${USER_RD} -P${PORT_RD} -p${PASS_RD} oride_data --skip-column-names -e"
    set time_zone = '+1:00';
    select
        -1,
        o.city_id,
        min(c.name) as city_name,
        date_format(from_unixtime(floor(o.create_time/600)*600), '%Y-%m-%d %H:%i:00') as order_time,
        date_format(from_unixtime(o.create_time), '%Y-%m-%d 00:00:00') as order_day,
        count(1) as orders,
        count(distinct o.user_id) as orders_user,
        sum(if(o.driver_id>0 and o.take_time>0,1,0)) as orders_pick,
        sum(if(o.status=4 or o.status=5,1,0)) as orders_finish,
        if(sum(if(o.driver_id>0 and o.take_time>0,1,0))>0,
            floor(sum(if(o.driver_id>0 and o.take_time>0, o.take_time-o.create_time, 0))/sum(if(o.driver_id>0 and o.take_time>0,1,0))), 0) as avg_pick,
        if(sum(if(o.pickup_time>0,1,0))>0,
            round(sum(if(o.pickup_time>0, o.pickup_time-o.take_time, 0))/60/sum(if(o.pickup_time>0,1,0)), 1), 0) as avg_take,
        count(if(o.status=6 and o.driver_id<>0 and o.cancel_role<>3 and o.cancel_role<>4, o.id, null)) as not_sys_cancel_orders,
        count(if(o.pickup_time>0, o.id, null)) as picked_orders,
        count(if(o.take_time>0, o.id, null)) as orders_accept
    from oride_data.data_order o left join oride_data.data_city_conf c
    on c.id = o.city_id
    where o.create_time>=unix_timestamp(date_format(now(),'%Y-%m-%d %H'))-7200 and
        o.create_time<floor(unix_timestamp()/600)*600 and
        c.id < 999000
    group by o.city_id, order_time, order_day
")
_eof

#3. type汇总
while read type city_id city daytime1 daytime2 day1 day2 orders orders_user orders_pick orders_finish avg_pick avg_take not_sys_cancel_orders picked_orders orders_accept; do
    mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e"
        insert into ${DATA_TABLE}
            (city_id, city_name, serv_type, order_time, daily, orders, orders_user, orders_pick, orders_finish, avg_pick, avg_take,not_sys_cancel_orders,picked_orders,orders_accept)
        values
            ('${city_id}', '${city}', '${type}', '${daytime1} ${daytime2}', '${day1} ${day2}', '${orders}', '${orders_user}', '${orders_pick}', '${orders_finish}', '${avg_pick}', '${avg_take}', '${not_sys_cancel_orders}', '${picked_orders}', '${orders_accept}')
        on duplicate key update
            city_name=values(city_name),
            orders=values(orders),
            orders_user=values(orders_user),
            orders_pick=values(orders_pick),
            orders_finish=values(orders_finish),
            avg_pick=values(avg_pick),
            avg_take=values(avg_take),
            not_sys_cancel_orders=values(not_sys_cancel_orders),
            picked_orders=values(picked_orders),
            orders_accept=values(orders_accept)"
done <<_eof
$(mysql -h${HOST_RD} -u${USER_RD} -P${PORT_RD} -p${PASS_RD} oride_data --skip-column-names -e"
    set time_zone = '+1:00';
    select
        serv_type,
        0,
        'all',
        date_format(from_unixtime(floor(create_time/600)*600), '%Y-%m-%d %H:%i:00') as order_time,
        date_format(from_unixtime(create_time), '%Y-%m-%d 00:00:00') as order_day,
        count(1) as orders,
        count(distinct user_id) as orders_user,
        sum(if(driver_id>0 and take_time>0,1,0)) as orders_pick,
        sum(if(status=4 or status=5,1,0)) as orders_finish,
        if(sum(if(driver_id>0 and take_time>0,1,0))>0,
            floor(sum(if(driver_id>0 and take_time>0, take_time-create_time, 0))/sum(if(driver_id>0 and take_time>0,1,0))), 0) as avg_pick,
        if(sum(if(pickup_time>0,1,0))>0,
            round(sum(if(pickup_time>0, pickup_time-take_time, 0))/60/sum(if(pickup_time>0,1,0)), 1), 0) as avg_take,
        count(if(status=6 and driver_id<>0 and cancel_role<>3 and cancel_role<>4, id, null)) as not_sys_cancel_orders,
        count(if(pickup_time>0, id, null)) as picked_orders,
        count(if(take_time>0, id, null)) as orders_accept
    from oride_data.data_order
    where create_time>=unix_timestamp(date_format(now(),'%Y-%m-%d %H'))-7200 and
        create_time<floor(unix_timestamp()/600)*600
    group by serv_type, order_time, order_day
")
_eof

#4. 城市、类型汇总
while read type city_id city daytime1 daytime2 day1 day2 orders orders_user orders_pick orders_finish avg_pick avg_take not_sys_cancel_orders picked_orders orders_accept; do
    mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e"
        insert into ${DATA_TABLE}
            (city_id, city_name, serv_type, order_time, daily, orders, orders_user, orders_pick, orders_finish, avg_pick, avg_take,not_sys_cancel_orders,picked_orders,orders_accept)
        values
            ('${city_id}', '${city}', '${type}', '${daytime1} ${daytime2}', '${day1} ${day2}', '${orders}', '${orders_user}', '${orders_pick}', '${orders_finish}', '${avg_pick}', '${avg_take}', '${not_sys_cancel_orders}', '${picked_orders}', '${orders_accept}')
        on duplicate key update
            city_name=values(city_name),
            orders=values(orders),
            orders_user=values(orders_user),
            orders_pick=values(orders_pick),
            orders_finish=values(orders_finish),
            avg_pick=values(avg_pick),
            avg_take=values(avg_take),
            not_sys_cancel_orders=values(not_sys_cancel_orders),
            picked_orders=values(picked_orders),
            orders_accept=values(orders_accept)"
done <<_eof
$(mysql -h${HOST_RD} -u${USER_RD} -P${PORT_RD} -p${PASS_RD} oride_data --skip-column-names -e"
    set time_zone = '+1:00';
    select
        o.serv_type,
        o.city_id,
        min(c.name) as city_name,
        date_format(from_unixtime(floor(o.create_time/600)*600), '%Y-%m-%d %H:%i:00') as order_time,
        date_format(from_unixtime(o.create_time), '%Y-%m-%d 00:00:00') as order_day,
        count(1) as orders,
        count(distinct o.user_id) as orders_user,
        sum(if(o.driver_id>0 and o.take_time>0,1,0)) as orders_pick,
        sum(if(o.status=4 or o.status=5,1,0)) as orders_finish,
        if(sum(if(o.driver_id>0 and o.take_time>0,1,0))>0,
            floor(sum(if(o.driver_id>0 and o.take_time>0, o.take_time-o.create_time, 0))/sum(if(o.driver_id>0 and o.take_time>0,1,0))), 0) as avg_pick,
        if(sum(if(o.pickup_time>0,1,0))>0,
            round(sum(if(o.pickup_time>0, o.pickup_time-o.take_time, 0))/60/sum(if(o.pickup_time>0,1,0)), 1), 0) as avg_take,
        count(if(o.status=6 and o.driver_id<>0 and o.cancel_role<>3 and o.cancel_role<>4, o.id, null)) as not_sys_cancel_orders,
        count(if(o.pickup_time>0, o.id, null)) as picked_orders,
        count(if(o.take_time>0, o.id, null)) as orders_accept
    from oride_data.data_order o left join oride_data.data_city_conf c
    on c.id = o.city_id
    where o.create_time>=unix_timestamp(date_format(now(),'%Y-%m-%d %H'))-7200 and
        o.create_time<floor(unix_timestamp()/600)*600 and
        c.id < 999000
    group by o.city_id, o.serv_type, order_time, order_day
")
_eof

CURR_TIMESTAMP=$(date +'%s')
PREV_10MIN=$((CURR_TIMESTAMP - 600))

#5. 累计完单数 汇总
while read type city_id city daytime1 daytime2 day1 day2 agg_orders_finish; do
    mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e"
        insert into ${DATA_TABLE}
            (city_id, city_name, serv_type, order_time, daily, agg_orders_finish)
        values
            ('${city_id}', '${city}', '${type}', '${daytime1} ${daytime2}', '${day1} ${day2}', '${agg_orders_finish}')
        on duplicate key update
            city_name=values(city_name),
            agg_orders_finish=values(agg_orders_finish)"
done <<_eof
$(mysql -h${HOST_RD} -u${USER_RD} -P${PORT_RD} -p${PASS_RD} oride_data --skip-column-names -e"
    set time_zone = '+1:00';
    select
        -1,
        0,
        'all',
        date_format(from_unixtime(floor(${PREV_10MIN}/600)*600), '%Y-%m-%d %H:%i:00') as order_time,
        date_format(from_unixtime(arrive_time), '%Y-%m-%d 00:00:00') as order_day,
        sum(if(status=4 or status=5,1,0)) as orders_finish
    from oride_data.data_order
    where arrive_time>=unix_timestamp(date_format(from_unixtime(${PREV_10MIN}),'%Y-%m-%d 00:00:00')) and
        arrive_time<floor(${CURR_TIMESTAMP}/600)*600
    group by order_time, order_day
")
_eof

#6. 累计完单数 城市汇总
while read type city_id city daytime1 daytime2 day1 day2 agg_orders_finish; do
    mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e"
        insert into ${DATA_TABLE}
            (city_id, city_name, serv_type, order_time, daily, agg_orders_finish)
        values
            ('${city_id}', '${city}', '${type}', '${daytime1} ${daytime2}', '${day1} ${day2}', '${agg_orders_finish}')
        on duplicate key update
            city_name=values(city_name),
            agg_orders_finish=values(agg_orders_finish)"
done <<_eof
$(mysql -h${HOST_RD} -u${USER_RD} -P${PORT_RD} -p${PASS_RD} oride_data --skip-column-names -e"
    set time_zone = '+1:00';
    select
        -1,
        o.city_id,
        min(c.name),
        date_format(from_unixtime(floor(${PREV_10MIN}/600)*600), '%Y-%m-%d %H:%i:00') as order_time,
        date_format(from_unixtime(o.arrive_time), '%Y-%m-%d 00:00:00') as order_day,
        sum(if(o.status=4 or o.status=5,1,0)) as orders_finish
    from oride_data.data_order o left join oride_data.data_city_conf c
    on c.id = o.city_id
    where o.arrive_time>=unix_timestamp(date_format(from_unixtime(${PREV_10MIN}),'%Y-%m-%d 00:00:00')) and
        o.arrive_time<floor(${CURR_TIMESTAMP}/600)*600 and
        c.id < 999000
    group by o.city_id, order_time, order_day
")
_eof

#7. 累计完单数 type汇总
while read type city_id city daytime1 daytime2 day1 day2 agg_orders_finish; do
    mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e"
        insert into ${DATA_TABLE}
            (city_id, city_name, serv_type, order_time, daily, agg_orders_finish)
        values
            ('${city_id}', '${city}', '${type}', '${daytime1} ${daytime2}', '${day1} ${day2}', '${agg_orders_finish}')
        on duplicate key update
            city_name=values(city_name),
            agg_orders_finish=values(agg_orders_finish)"
done <<_eof
$(mysql -h${HOST_RD} -u${USER_RD} -P${PORT_RD} -p${PASS_RD} oride_data --skip-column-names -e"
    set time_zone = '+1:00';
    select
        serv_type,
        0,
        'all',
        date_format(from_unixtime(floor(${PREV_10MIN}/600)*600), '%Y-%m-%d %H:%i:00') as order_time,
        date_format(from_unixtime(arrive_time), '%Y-%m-%d 00:00:00') as order_day,
        sum(if(status=4 or status=5,1,0)) as orders_finish
    from oride_data.data_order
    where arrive_time>=unix_timestamp(date_format(from_unixtime(${PREV_10MIN}),'%Y-%m-%d 00:00:00')) and
        arrive_time<floor(${CURR_TIMESTAMP}/600)*600
    group by serv_type, order_time, order_day
")
_eof

#8. 累计完单数, 城市、type汇总
while read type city_id city daytime1 daytime2 day1 day2 agg_orders_finish; do
    mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e"
        insert into ${DATA_TABLE}
            (city_id, city_name, serv_type, order_time, daily, agg_orders_finish)
        values
            ('${city_id}', '${city}', '${type}', '${daytime1} ${daytime2}', '${day1} ${day2}', '${agg_orders_finish}')
        on duplicate key update
            city_name=values(city_name),
            agg_orders_finish=values(agg_orders_finish)"
done <<_eof
$(mysql -h${HOST_RD} -u${USER_RD} -P${PORT_RD} -p${PASS_RD} oride_data --skip-column-names -e"
    set time_zone = '+1:00';
    select
        o.serv_type,
        o.city_id,
        min(c.name),
        date_format(from_unixtime(floor(${PREV_10MIN}/600)*600), '%Y-%m-%d %H:%i:00') as order_time,
        date_format(from_unixtime(o.arrive_time), '%Y-%m-%d 00:00:00') as order_day,
        sum(if(o.status=4 or o.status=5,1,0)) as orders_finish
    from oride_data.data_order o left join oride_data.data_city_conf c
    on c.id = o.city_id
    where o.arrive_time>=unix_timestamp(date_format(from_unixtime(${PREV_10MIN}),'%Y-%m-%d 00:00:00')) and
        o.arrive_time<floor(${CURR_TIMESTAMP}/600)*600 and
        c.id < 999000
    group by o.city_id, o.serv_type, order_time, order_day
")
_eof



#CURR_TIMESTAMP=$(date +'%s')
#PREV_10MIN=$((CURR_TIMESTAMP - 600))
#PREV_1HOUR=$(date +'%Y-%m-%d %H:00:00' --date="3 hour ago")
#PREV_HOUR_TIMESTAMP=$(date +'%s' --date="${PREV_1HOUR}")

#while ((${PREV_HOUR_TIMESTAMP} <= ${PREV_10MIN})); do
#    DAY_10MIN=$(date +'%Y-%m-%d %H:%M:00' --date=@${PREV_HOUR_TIMESTAMP})
#    DAY_DAY=$(date +'%Y-%m-%d 00:00:00' --date=@${PREV_HOUR_TIMESTAMP})
#    PREV_HOUR_TIMESTAMP=$((PREV_HOUR_TIMESTAMP + 600))

#    mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e "
#        insert into ${DATA_TABLE} (city_id, serv_type, order_time, daily, agg_orders_finish)
#        (select
#            city_id, serv_type, '${DAY_10MIN}', '${DAY_DAY}', sum(orders_finish)
#        from ${DATA_TABLE}
#        where order_time>'${DAY_DAY}' and order_time<='${DAY_10MIN}'
#        group by city_id, serv_type
#        )
#        on duplicate key update agg_orders_finish=values(agg_orders_finish)
#     "
#done

#在线司机数
CURR_TIMESTAMP=$(date +'%s')
PREV_10MIN=$((CURR_TIMESTAMP - 600))
PREV_1HOUR=$(date +'%Y-%m-%d %H:00:00' --date="3 hour ago")
CURR_TIME=$(date +'%Y-%m-%d %H:%M:00')
#每城市每类型
mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e"
    insert into ${DATA_TABLE} (city_id, serv_type, order_time, daily, drivers_serv, drivers_orderable)
    (
        select
            City, type, online_time, date_format(online_time, '%Y-%m-%d 00:00:00'), drivers_online, driver_orderable
        from driver_online
        where online_time>='${PREV_1HOUR}' and
            online_time<='${CURR_TIME}'
    )
    on duplicate key update
        drivers_serv = values(drivers_serv),
        drivers_orderable = values(drivers_orderable)
"
#每城市总类型
mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e"
    insert into ${DATA_TABLE} (city_id, serv_type, order_time, daily, drivers_serv, drivers_orderable)
    (
        select
            City, -1, online_time, date_format(online_time, '%Y-%m-%d 00:00:00'), sum(drivers_online), sum(driver_orderable)
        from driver_online
        where online_time>='${PREV_1HOUR}' and
            online_time<='${CURR_TIME}'
        group by City, online_time
    )
    on duplicate key update
        drivers_serv = values(drivers_serv),
        drivers_orderable = values(drivers_orderable)
"
#城市、类型汇总
mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e"
    insert into ${DATA_TABLE} (city_id, serv_type, order_time, daily, drivers_serv, drivers_orderable)
    (
        select
            0, -1, online_time, date_format(online_time, '%Y-%m-%d 00:00:00'), sum(drivers_online), sum(driver_orderable)
        from driver_online
        where online_time>='${PREV_1HOUR}' and
            online_time<='${CURR_TIME}'
        group by online_time
    )
    on duplicate key update
        drivers_serv = values(drivers_serv),
        drivers_orderable = values(drivers_orderable)
"
#每类型汇总
mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e"
    insert into ${DATA_TABLE} (city_id, serv_type, order_time, daily, drivers_serv, drivers_orderable)
    (
        select
            0, type, online_time, date_format(online_time, '%Y-%m-%d 00:00:00'), sum(drivers_online), sum(driver_orderable)
        from driver_online
        where online_time>='${PREV_1HOUR}' and
            online_time<='${CURR_TIME}'
        group by type, online_time
    )
    on duplicate key update
        drivers_serv = values(drivers_serv),
        drivers_orderable = values(drivers_orderable)
"
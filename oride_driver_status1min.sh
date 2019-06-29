#!/usr/bin/env bash

HOST_RD=$1
PORT_RD=$2
USER_RD=$3
PASS_RD=$4
HOST_BI=$5
PORT_BI=$6
USER_BI=$7
PASS_BI=$8


DATA_VALUES=""
DATA_COUNTS=0
while read daytime1 daytime2 day1 day2 driver_id serv_model serv_status city; do
    DATA_COUNTS=$((DATA_COUNTS + 1))
    if [ -z "${DATA_VALUES}" ];then
        DATA_VALUES="('${daytime1} ${daytime2}', '${day1} ${day2}', ${driver_id}, ${serv_model}, ${serv_status}, ${city})"
    else
        DATA_VALUES="${DATA_VALUES},('${daytime1} ${daytime2}', '${day1} ${day2}', ${driver_id}, ${serv_model}, ${serv_status}, ${city})"
    fi
    if [ ${DATA_COUNTS} -gt 1000 ];then
        mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e"
            insert into oride_driver_status_1min (create_time, daily, driver_id, serv_model, serv_status, city) values ${DATA_VALUES}"
        DATA_VALUES=""
        DATA_COUNTS=0
    fi
done <<_eof
    $(mysql -h${HOST_RD} -u${USER_RD} -P${PORT_RD} -p${PASS_RD} oride_data --skip-column-names -e"set time_zone = '+1:00'; select date_format(now(), '%Y-%m-%d %H:%i:%s') as create_time, date_format(now(), '%Y-%m-%d 00:00:00') as daily, id, serv_mode, serv_status, 0 from oride_data.data_driver_extend")
_eof

if [ ${DATA_COUNTS} -gt 0 ];then
    mysql -h${HOST_BI} -u${USER_BI} -P${PORT_BI} -p${PASS_BI} bi -e"
        insert into oride_driver_status_1min (create_time, daily, driver_id, serv_model, serv_status, city) values ${DATA_VALUES}"
    DATA_VALUES=""
    DATA_COUNTS=0
fi
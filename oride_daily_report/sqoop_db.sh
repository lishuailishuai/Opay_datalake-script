#!/usr/bin/env bash
sqoop import --connect "jdbc:mysql://10.52.123.212:3306/oride_data?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8" \
--username oride \
--password 3r8Kn@483Gh6g3p9 \
--table $1 \
--target-dir ufile://opay-datalake/oride/db/$1/dt=$2/ \
--fields-terminated-by "\001" \
--lines-terminated-by "\n" \
--hive-delims-replacement " " \
--delete-target-dir  \
--compression-codec=snappy
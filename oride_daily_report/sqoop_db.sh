#!/usr/bin/env bash
sqoop import --connect "jdbc:mysql://10.52.166.51:13306/oride_data?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8" \
--username bigdata \
--password S5o6KOjEVI \
--table $1 \
--target-dir ufile://opay-datalake/oride/db/$1/dt=$2/ \
--fields-terminated-by "\001" \
--lines-terminated-by "\n" \
--hive-delims-replacement " " \
--delete-target-dir  \
--compression-codec=snappy
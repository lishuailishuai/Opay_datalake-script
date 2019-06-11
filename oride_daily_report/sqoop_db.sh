#!/usr/bin/env bash
sqoop import --connect "jdbc:mysql://$1/$2?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8" \
--username $3 \
--password $4 \
--table $5 \
--target-dir ufile://opay-datalake/oride/db/$6/dt=$7/ \
--fields-terminated-by "\001" \
--lines-terminated-by "\n" \
--hive-delims-replacement " " \
--delete-target-dir  \
--compression-codec=snappy
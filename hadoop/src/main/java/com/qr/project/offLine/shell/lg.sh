#!/bin/bash

for host in hadoop102 hadoop103;
do
    echo "=============$host============="
    ssh $host "cd /project/bigData/applog/; java -jar gmall2020-mock-log-2021-10-10.jar >/dev/null 2>&1 &"
done

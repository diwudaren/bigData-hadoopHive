#!/bin/bash

flume(){
    statusName=$([ $1 == "start" ] && echo -n "启动" || echo -n "停止")
    statusIf=$([ $1 == "start" ] && echo -n "-lt 1" || echo -n "-gt 0")
    for host in hadoop102
    do
        status_flume $host
        if [ $? $statusIf ]
        then
            echo "==============$statusName $host 采集flume=============="
            if [ $1 == "start" ]
            then
                echo "==============bin/flume-ng agent -n a1 -c conf/ -f job/file_to_kafka.conf=============="
                ssh $host "cd /soft/flume; nohup bin/flume-ng agent -n a1 -c conf/ -f job/file_to_kafka.conf >/dev/null 2>&1 &"
            else
                echo "==============ps -ef |grep ile_to_kafka.conf |grep -v grep | awk '{print $2}' | xargs -n1 kill -9=============="
                ssh $host "ps -ef |grep ile_to_kafka.conf |grep -v grep | awk '{print \$2}' | xargs -n1 kill -9"
            fi
        else
            echo "=============以$statusName $host 采集flume=============="
        fi
    done

}

status_flume(){
    result=`ssh $1 ps -ef | grep ile_to_kafka.conf | grep -v grep | wc -l`
    return $result
}

case $1 in
"start")
    flume $1
;;
"stop")
    flume $1
;;
esac
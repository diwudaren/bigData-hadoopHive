#!/bin/bash



flume(){
    statusName=$([ $1 == "start" ] && echo -e "启动" || echo -e "停止")
    statusIf=$([ $1 == "start" ] && echo -e "-lt 1" || echo -e "-gt 0")
    for host in hadoop102
    do
        status_flume $host
        if [ $? $statusIf ]
        then
            echo "===============$statusName $host Flume==============="
            if [ $1 == "start" ]
            then
                ssh $host "cd /soft/flume; nohup flume-ng agent -n a1 -c conf/ -f job/kafka_to_hdfs_log.conf >/dev/null 2>&1 &"
            else
                ssh $host `ps -ef | grep kafka_to_hdfs_log | grep -v grep | awk '{print \$2}' | xargs -n1 kill -9`
            fi
        else
            echo "===============以$statusName $host Flume==============="
        fi

    done
}

status_flume(){
   result=`ssh $1 ps -ef | grep kafka_to_hdfs_log | grep -v grep | wc -l`
   return $result
}

case $1 in
"start" | "stop")
    flume $1
;;
esac
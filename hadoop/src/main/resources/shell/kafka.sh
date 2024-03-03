#!/bin/bash

if [ $# -lt 1 ]
then
    echo -e "请输入参数：\n start  启动kafka集群;\n stop  停止kafka集群;\n" && exit
    exit
fi

kafka () {
    path="/soft/kafka"
    kafkaCmd=$([ $1 == 'start' ] && echo -n $path"/bin/kafka-server-start.sh -daemon" $path"/config/server.properties" || ([ $1 == 'stop' ] && echo -n $path"/bin/kafka-server-stop.sh"))
    for host in hadoop102 hadoop103 hadoop104
        do
            echo "---------- $1 $host 的kafka ----------"
            echo $host $kafkaCmd
            ssh $host $kafkaCmd
        done

}

case $1 in
"start")
    kafka "start"
;;
"stop")
    kafka "stop"
;;
*)
    echo -e "---------- 请输入正确的参数 ----------\n"
    echo -e "start  启动kafka集群;\n stop  停止kafka集群;\n" && exit
;;
esac
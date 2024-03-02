#!/bin/bash

if [ $# -lt 1 ]; then
  echo ""
  exit
fi

zk() {
  for host in hadoop102 hadoop103 hadoop104; do
    status=$([ $1 == 'start' ] && echo -n "启动" || ([ $1 == 'stop' ] && echo -n "停止") || echo -n "状态")
    echo "---------- zookeeper $host $status ------------"
    ssh $host "/soft/zookeeper/bin/zkServer.sh $1"
  done
}

case $1 in
"start")
  zk $1
  ;;
"stop")
  zk $1
  ;;
"restart")
  zk "stop"
  sleep 3
  zk "start"
  ;;
"status")
  zk $1
  ;;
"*")
  echo "Input Args Error..."
;;
esac

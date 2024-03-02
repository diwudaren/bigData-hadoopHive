#!/bin/bash

if [ $# -lt 1 ]; then
  echo "No Args Input..."
  exit
fi

start() {
  echo " ======启动 hadoop集群 ======="
  echo " --------------- 启动 hdfs ---------------"
  ssh hadoop102 "/soft/hadoop/sbin/start-dfs.sh"
  echo " --------------- 启动 yarn ---------------"
  ssh hadoop103 "/soft/hadoop/sbin/start-yarn.sh"
}

stop() {
  echo " ======关闭 hadoop集群 ======="
  echo " --------------- 关闭 yarn ---------------"
  ssh hadoop103 "/soft/hadoop/sbin/stop-yarn.sh"
  echo " --------------- 关闭 hdfs ---------------"
  ssh hadoop102 "/soft/hadoop/sbin/stop-dfs.sh"
}

case $1 in
"start")
  start
  ;;
"stop")
  stop
  ;;
"restart")
  stop
  sleep 3
  start
  ;;
*)
  echo "Input Args Error..."
  ;;
esac

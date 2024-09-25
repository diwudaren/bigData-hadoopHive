#!/bin/bash

if [ $# -lt 1 ]; then
     echo -e "请输入参数：\n start  启动kafka集群;\n stop  停止kafka集群;\n" && exit
    exit
fi

HBASE_HOME=/soft/hbase

status_hbase(){
  result=`ps -aux |grep hbase |grep -v grep |grep foreground_start | wc -l`
  return $result
}

hbase() {
  status_hbase
  str=$?
  [[ "start" == $1 && $str -gt 0 ]] && echo -e "hbase hadoop102 已启动" && exit || [[ "stop" == $1 && $str -lt 1 ]] && echo -e "hbase hadoop102 未启动" && exit
  status=$([ "start" == $1 ] && echo -n "启动" || ([ "stop" == $1 ] && echo -n "停止") || echo -n "状态")
  start_and_stop=$([ "start" == $1 ] && echo -n "start-hbase.sh" || ([ "stop" == $1 ] && echo -n "stop-hbase.sh"))
  echo "---------- hbase hadoop102 $status $start_and_stop------------"
  ssh hadoop102 $HBASE_HOME/bin/$start_and_stop
}

case $1 in
  "start" | "stop")
   hbase $1
  ;;
"status")
  status_hbase
  [[ $? -gt 0 ]] && echo -e "hbase hadoop102 已启动" || echo -e "hbase hadoop102 未启动"

;;
"*")
  echo "Input Args Error..."
;;
esac
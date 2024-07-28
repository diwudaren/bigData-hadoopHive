#!/bin/bash
if [ $# -lt 1 ]
then
    echo -e "请输入参数：\n start  启动redis;\n stop  停止redis;\n stop  查看redis状态;\n"
    exit
fi
function check_point()
{
    pid=$(ps -ef 2>/dev/null | grep -v grep | grep -i $1 | awk '{print $2}')
    ppid=$(netstat -nltp 2>/dev/null | grep $2 | awk '{print $7}' | cut -d '/' -f 1)
    echo $pid
    [[ "$pid" =~ "$ppid" ]] && [ "$ppid" ] && return 0 || return 1
}

REDIS_PATH=/soft/redis
case $1 in
"start")
    metapid=$(check_point redis-server 6379)
    cmd="$REDIS_PATH/bin/redis-server $REDIS_PATH/redis.conf"
    [ -z "$metapid" ] && (echo "Redis 服务启动" && eval $cmd) || echo "Redis 服务已启动"
;;
"stop")
    metapId=$(check_point redis-server 6379)
    [ "$metapId" ] && (echo "Redis 服务关闭" && $REDIS_PATH/bin/redis-cli shutdown) || echo "Redis 服务未启动"
;;
"status")
    check_point redis 6379 >/dev/null && echo "Redis 服务运行正常" || echo "Redis 服务运行异常"
;;
"*")
  echo "Input Args Error..."
;;
esac

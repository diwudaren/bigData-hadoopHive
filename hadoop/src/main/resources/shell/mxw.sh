#!/bin/bash

MAXWELL_HOME=/soft/maxwell

start_maxwell(){
    status_maxwell
    if [[ $? -lt 1 ]]; then
        echo "启动Maxwell"
        $MAXWELL_HOME/bin/maxwell -config $MAXWELL_HOME/config.properties --daemon
    else
        echo "Maxwell 正在运行"
    fi
}
status_maxwell(){
    result=`ps -ef | grep com.zendesk.maxwell.Maxwel | grep -v grep | wc -l`
    return $result
}

stop_maxwell(){
        status_maxwell
        if [[ $? -gt 0 ]]; then
            echo "停止Maxwell"
            ps -ef | grep com.zendesk.maxwell.Maxwel | grep -v grep | awk '{print $2}' | xargs kill -9
        else
            echo "Maxwell 未在运行"
        fi
}

case $1 in
    start)
        start_maxwell
    ;;
    stop)
        stop_maxwell
    ;;
    restart)
       stop_maxwell
       start_maxwell
    ;;
esac
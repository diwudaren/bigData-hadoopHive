#!/bin/bash

DORIS_HOME=/opt/module/apache-doris-1.0.0

if [[ "$1"s = "start"s ]]
then
	echo "========================> 启动 frontends <========================"
	for host in hadoop102 hadoop103 hadoop104
	do
	    echo "----------> 启动 $host 的 frontend <----------"
	    ssh $host $DORIS_HOME/fe/bin/start_fe.sh --daemon
	done

	echo "========================> 启动 backends <========================"
	for host in hadoop102 hadoop103 hadoop104
	do
	    echo "----------> 启动 $host 的 backend <----------"
	    ssh $host $DORIS_HOME/be/bin/start_be.sh --daemon
	done

	echo "========================> 启动 brokers <========================"
	for host in hadoop102 hadoop103 hadoop104
	do
	    echo "----------> 启动 $host 的 brokers <----------"
	    ssh $host $DORIS_HOME/apache_hdfs_broker/bin/start_broker.sh --daemon
	done
elif [[ "$1"s = "stop"s ]]
then
	echo "========================> 停止 brokers <========================"
	for host in hadoop102 hadoop103 hadoop104
	do
	    echo "----------> 停止 $host 的 brokers <----------"
	    ssh $host $DORIS_HOME/apache_hdfs_broker/bin/stop_broker.sh
	done
	echo "========================> 停止 backends <========================"
	for host in hadoop102 hadoop103 hadoop104
	do
	    echo "----------> 停止 $host 的 backend <----------"
	    ssh $host $DORIS_HOME/be/bin/stop_be.sh
	done

	echo "========================> 停止 frontends <========================"
	for host in hadoop102 hadoop103 hadoop104
	do
	    echo "----------> 停止 $host 的 frontend <----------"
	    ssh $host $DORIS_HOME/fe/bin/stop_fe.sh
	done
elif [[ "$1"s = "status"s ]]
then
	echo "========================> frontends 状态 <========================"
	for host in hadoop102 hadoop103 hadoop104
	do
	    echo "----------> $host 的 frontend <----------"
	    ssh $host jps -l | grep PaloFe | grep -v grep
	done

	echo "========================> backends 状态 <========================"
	for host in hadoop102 hadoop103 hadoop104
	do
	    echo "----------> $host 的 backend <----------"
	    ssh $host ps -ef | grep palo_be | grep -v grep
	done

	echo "========================> brokers 状态 <========================"
	for host in hadoop102 hadoop103 hadoop104
	do
	    echo "----------> $host 的 brokers <----------"
	    ssh $host ps -ef | grep org.apache.doris.broker.hdfs.BrokerBootstrap | grep -v grep
	done
else
	echo "ARGUMENTS ERROR !!!
USAGE: doris-server.sh START | STOP | STATUS "
fi
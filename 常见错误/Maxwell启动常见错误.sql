
-- maxwell启动后报错：
-- 1、com.github.shyiko.mysql.binlog.network.ServerException: Could not find first log file name in binary log index file at com.github.s
-- 2、com.github.shyiko.mysql.binlog.network.ServerException: Client requested master to start replication from position > file size
-- 解决办法
-- 	1、MySQL的file和Maxwell的binlog_file对不上，将MySQL的file和Maxwell的binlog_file对上
-- 	2、MySQL的position和Maxwell的binlog_position对不上，将MySQL的position和Maxwell的binlog_position对上
SHOW MASTER STATUS
-- 	1、file 			2、position
-- 	mysql-bin.000020 	785463

SELECT * FROM positions
-- 	1、binlog_file 		2、binlog_position
-- 	mysql-bin.000004 	2648430
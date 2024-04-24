flinkcdc多表实时同步任务代码，打包成jar包后，可以作为flink任务运行，部署在flink单机、集群或者flink-on-yarn
前提条件：mysql开启binlog
功能简介：任务可以实现数据源与目标源的多表之间的表名的映射以及字段的映射来同步数据，其中source不会锁表，且代码保证数据的一致性、顺序性、断点恢复功能，自动清表功能等
任务启动命令：
./bin/flink run ./flinkcdc-mysql2oracle-SNAPSHOT.jar
--job_name       任务名称
test
--source_host    数据源主机
127.0.0.1
--source_port    数据源端口
3306
--source_username    数据源账号
root3
--source_password    数据源密码
xw123456 
--source_database    数据源数据库
flinkcdc
--source_table       同步的表列表，多表之间用逗号隔开
user
--sink_host          目标源主机
xx.xx.xx.xx
--sink_port          目标源端口
1521
--sink_username      目标源账号
ods
--sink_password      目标源密码
ODS#0903
--sink_database      目标源数据库
rodsdb
--sink_schema        目标源模式
ODS
--intervals          数据刷新间隔ms
3000
--batch_size         数据批次大小
8096
--operation          启动任务（restart是断点恢复任务）
start

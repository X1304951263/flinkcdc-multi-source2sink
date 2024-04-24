该项目是使用datastream来实现实时同步，而不是flinksql，由于flinksql需要写大量sql且不够通用化，所以采用datastream来高度自定义统一的任务模版，只需要改变参数即可实现任何mysql-oracle的同步功能<br /> 
<br /> 
flinkcdc多表实时同步任务代码，打包成jar包后，可以作为flink任务运行，部署在flink单机、集群或者flink-on-yarn <br /> <br /> 
前提条件：mysql开启binlog  <br/> <br /> 
功能简介：任务可以实现数据源与目标源的多表之间的表名的映射以及字段的映射来同步数据，其中source不会锁表，且代码保证数据的一致性、顺序性、断点恢复功能，自动清表功能等 <br/> <br /> 
任务启动命令： <br/> <br /> 
./bin/flink run ./flinkcdc-mysql2oracle-SNAPSHOT.jar  <br/> 
--job_name       任务名称  <br/> 
test   <br/> 
--source_host    数据源主机 <br/> 
127.0.0.1<br/> 
--source_port    数据源端口<br/> 
3306<br/> 
--source_username    数据源账号<br/> 
root3<br/> 
--source_password    数据源密码<br/> 
xw123456 <br/> 
--source_database    数据源数据库<br/> 
flinkcdc<br/> 
--source_table       同步的表列表，多表之间用逗号隔开<br/> 
user<br/> 
--sink_host          目标源主机<br/> 
xx.xx.xx.xx<br/> 
--sink_port          目标源端口<br/> 
1521<br/> 
--sink_username      目标源账号<br/> 
ods<br/> 
--sink_password      目标源密码<br/> 
ODS#0903<br/> 
--sink_database      目标源数据库<br/> 
rodsdb<br/> 
--sink_schema        目标源模式<br/> 
ODS<br/> 
--intervals          数据刷新间隔ms<br/> 
3000<br/> 
--batch_size         数据批次大小<br/> 
8096<br/> 
--operation          启动任务（restart是断点恢复任务）<br/> 
start<br/> 

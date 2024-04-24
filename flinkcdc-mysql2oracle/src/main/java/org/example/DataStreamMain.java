package org.example;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.example.common.CommonConfig;
import org.example.common.Util;
import org.example.sink.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;

public class DataStreamMain {
    public static void main(String[] args) throws Exception {
        Util.parseArgs(args);
        //Util.checkRequiredParams();

        String tbList = "";
        String[] tbs = CommonConfig.SourceTable.split(",");  //user1,user2

        String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&useUnicode=true&characterEncoding=UTF-8&allowPublicKeyRetrieval=true",
                CommonConfig.SourceHost, CommonConfig.SourcePort, CommonConfig.SourceDatabase);

        String sinkUrl = String.format("jdbc:oracle:thin:@%s:%d:%s",
                CommonConfig.SinkHost, CommonConfig.SinkPort, CommonConfig.SinkDatabase);

        //源表所有字段及类型
        Map<String,Map<String,String>> tableFieldsWithTypeMap = DBTabField.GetTableFieldsWithType(
                url, CommonConfig.SourceDatabase, CommonConfig.SourceUser, CommonConfig.SourcePassword, tbs);

        //源表所有字段列表
        for (String tableName : tbs) {
            CommonConfig.tableFieldsMap.put(tableName,
                    new ArrayList<>(tableFieldsWithTypeMap.get(tableName).keySet()));
        }

        //解析表和字段的自定义映射关系
        CommonConfig.ParseTabMapping(CommonConfig.Tab2Tab);

        for (String s : tbs) {
            tbList += CommonConfig.SourceDatabase + "." + s + ",";
        }
        tbList = tbList.substring(0, tbList.length() - 1);    //db.user1,db.user2

        //源表和目标表的映射
        Map<String, String> tablesMap = ParamsBuilder.sourceTable2TargetTable(tbs);
        String[] sinkTables = new String[tablesMap.size()];
        int i = 0;
        for(String key : tablesMap.keySet()){
            sinkTables[i] = tablesMap.get(key);
            i++;
        }

        Map<String,List<String>> priKeyOrUniKeyMap = DBTabField.GetPriKeyOrUniKey(
                url,CommonConfig.SourceDatabase,
                CommonConfig.SourceUser,CommonConfig.SourcePassword,tbs);

        Map<String, Map<String, String>> fieldsMapping = ParamsBuilder.sourceField2TargetField(tablesMap);

        //目标表的字段列表
        Map<String, List<String>> sinkTableColumns = DBTabField
                .GetSinkTableFields(sinkUrl,CommonConfig.SinkSchema,
                        CommonConfig.SinkUser,CommonConfig.SinkPassword,sinkTables);

        for(String tb : tbs){
            List<String> sourceColumns = CommonConfig.tableFieldsMap.get(tb);  //user1,user2
            List<String> sinkColumns = sinkTableColumns.get(tablesMap.get(tb));   //USER1
            Map<String, String> fields = fieldsMapping.get(tb);              //user1-USER1
            for(int j = 0; j < sourceColumns.size(); j++){
                String column = fields.get(sourceColumns.get(j));
                if(!sinkColumns.contains(column)){
                    sourceColumns.remove(j);
                    j--;
                }
            }
        }

        //清除表
        if(CommonConfig.operation.equals("start")){
            truncateTables(sinkUrl,tablesMap);
        }


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties debeziumProperties = new Properties();
        //debeziumProperties.setProperty("converters", "dateConverters");
        //debeziumProperties.setProperty("dateConverters.type", "org.example.FieldsTypeConverter");
        debeziumProperties.setProperty("max.poll.interval.ms", "20000");

        //flinkConfig.setString("scan.incremental.snapshot.enabled", "true");

        //import com.ververica.cdc.connectors.mysql.source.MySqlSource;   默认无锁
        //import com.ververica.cdc.connectors.mysql.MySqlSource;        弃用，别用
        MySqlSource<JSONObject> sourceFunction = MySqlSource.<JSONObject>builder()
                .hostname(CommonConfig.SourceHost)
                .port(CommonConfig.SourcePort)
                .databaseList(CommonConfig.SourceDatabase) // set captured database
                .tableList(tbList) // 表名前跟上数据库名.  否则任务不报错，但是读不到数据
                .username(CommonConfig.SourceUser)
                .password(CommonConfig.SourcePassword)
                .debeziumProperties(debeziumProperties)
                .deserializer(new OracleBinlogDeserialization())
                //.serverTimeZone("UTC")
                .startupOptions(StartupOptions.initial()) // 设置读取模式,默认为从头读
                .build();



        DataStream<JSONObject> inputStream = env.fromSource(sourceFunction,
                WatermarkStrategy.noWatermarks(), "MySQL Source");


        OracleSink sink = new OracleSink(
                new JdbcOutput(
                        JdbcExecutionOptions.builder()
                                .setBatchSize(CommonConfig.batchSize)
                                .setBatchIntervalMs(CommonConfig.intervals)
                                .build(),
                        JdbcConnectionOptions.builder()
                                .url(sinkUrl)
                                .user(CommonConfig.SinkUser)
                                .password(CommonConfig.SinkPassword)
                                .build(),
                        tablesMap,
                        tableFieldsWithTypeMap,
                        priKeyOrUniKeyMap,
                        fieldsMapping,
                        CommonConfig.tableFieldsMap,
                        CommonConfig.SinkSchema,
                        CommonConfig.JobName,
                        new AlertUtil()
                )
        );


        inputStream.keyBy(value -> value.getString("key"))
                .windowAll(GlobalWindows.create())
                //数据达到maxElementCount条或者每个窗口的时间到了intervals毫秒，满足其一则触发窗口处理
                .trigger(new MyTrigger(CommonConfig.batchSize,CommonConfig.intervals))
                .apply(new MyWindowFunction())
                .setParallelism(1)
                .addSink(sink)
                .setParallelism(1)
                .name("OracleSink"); // 自定义窗口处理函数
        env.execute(CommonConfig.JobName);
    }

    public static final void truncateTables(String sinkUrl,Map<String,String> tablesMap) throws Exception {
        try {
            Class.forName("oracle.jdbc.OracleDriver");
            Connection connection = DriverManager
                    .getConnection(sinkUrl,
                            CommonConfig.SinkUser,
                            CommonConfig.SinkPassword);

            for(String table : tablesMap.keySet()){
                String sql = String.format("TRUNCATE TABLE \"%s\".\"%s\"", CommonConfig.SinkSchema, tablesMap.get(table));
                try {
                    Statement statement = connection.createStatement();
                    statement.execute(sql);
                    statement.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }
}

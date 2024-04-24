package org.example;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.batch.DorisBatchSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.example.common.CommonConfig;
import org.example.common.Util;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;


public class DataStreamMain {
    public static void main(String[] args) throws Exception {

//        //1. 解析参数
        Util.parseArgs(args);
       // Util.checkRequiredParams();

        String tbList = "";
        String[] tbs = CommonConfig.SourceTable.split(",");  //user1,user2
        //获取源表的所有字段名称
        CommonConfig.tableFieldsMap = DBTabField.GetTableFields(CommonConfig.SourceHost, CommonConfig.SourcePort,
                CommonConfig.SourceDatabase, CommonConfig.SourceUser, CommonConfig.SourcePassword, tbs);
        //解析表和字段的自定义映射关系
        CommonConfig.ParseTabMapping(CommonConfig.Tab2Tab);
        Map<String,String> columnsMap = CommonConfig.GetColumns(tbs);
        for (String s : tbs) {
            tbList += CommonConfig.SourceDatabase + "." + s + ",";
        }
        tbList = tbList.substring(0, tbList.length() - 1);    //db.user1,db.user2

        Map<String,String> tbsMap = ParamsBuilder.sourceTable2TargetTable(tbs);

        //清除表
        if(CommonConfig.operation.equals("start")){

            String url = String.format("jdbc:mysql://%s:%d/%s",
                    CommonConfig.SinkHost, 9030, CommonConfig.SinkDatabase);
            truncateTables(url,tbsMap);
        }


        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("converters", "dateConverters");
        debeziumProperties.setProperty("dateConverters.type", "org.example.MySqlDateTimeConverter");


        SourceFunction<JSONObject> sourceFunction = MySqlSource.<JSONObject>builder()
                .hostname(CommonConfig.SourceHost)
                .port(CommonConfig.SourcePort)
                .databaseList(CommonConfig.SourceDatabase) // set captured database
                .tableList(tbList) // 表名前跟上数据库名.  否则任务不报错，但是读不到数据
                .username(CommonConfig.SourceUser)
                .password(CommonConfig.SourcePassword)
                //.serverTimeZone("Asia/Shanghai")
                .debeziumProperties(debeziumProperties)
                .deserializer(new DorisBinlogDeserialization())
                .startupOptions(StartupOptions.initial()) // 设置读取模式,默认为从头读
                .build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建数据流
        DataStream<JSONObject> inputStream = env.addSource(sourceFunction);

        for(String s : tbs) {
            DataStream<String> tmp = inputStream.filter(value ->
                            value.getString("tableName").equals(s))
                    .process(new ProcessFunction<JSONObject, String>() {
                        @Override
                        public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                            String type = value.getString("type");
                            if ("create".equals(type) || "read".equals(type) || "update".equals(type)) {
                                //System.out.println(value.getJSONObject("after").toString());
                                out.collect(value.getJSONObject("after").toString());
                            }else{
                                out.collect(value.getJSONObject("before").toString());
                            }
                        }
                    });

            Properties properties = new Properties();
            properties.put("format", "json"); //默认为json格式插入数据
            properties.put("read_json_by_line", "true");
            properties.put("column_separator", ",");
            properties.put("columns", columnsMap.get(s) + ",eventtime,__DORIS_DELETE_SIGN__");

            DorisExecutionOptions dorisExecutionOptions = DorisExecutionOptions
                    .builder().setStreamLoadProp(properties)
                    .setBatchMode(true)
                    .setMaxRetries(3)
                    .setDeletable(true)
                    .setBufferFlushMaxRows(CommonConfig.batchSize)
                    .setBufferFlushIntervalMs(CommonConfig.intervals).build();

            DorisBatchSink.Builder<String> builder = DorisBatchSink.builder();

            DorisOptions.Builder dorisBuilder = DorisOptions.builder();

            dorisBuilder.setFenodes(CommonConfig.SinkHost + ":" + CommonConfig.SinkPort)
                    .setTableIdentifier(CommonConfig.SinkDatabase + "." + tbsMap.get(s))
                    .setUsername(CommonConfig.SinkUser)
                    .setPassword(CommonConfig.SinkPassword);

            builder.setDorisReadOptions(DorisReadOptions.builder().build())
                    .setDorisExecutionOptions(dorisExecutionOptions)
                    .setSerializer(new SimpleStringSerializer()) //serialize according to string
                    .setDorisOptions(dorisBuilder.build());

            tmp.sinkTo(builder.build()).setParallelism(1);
        }
        env.execute(CommonConfig.JobName);
    }

    public static final void truncateTables(String sinkUrl,Map<String,String> tablesMap) {
        try (Connection connection = DriverManager.getConnection(sinkUrl,
                CommonConfig.SinkUser, CommonConfig.SinkPassword);

             Statement statement = connection.createStatement()) {

            for(String tableName : tablesMap.keySet()) {
                String sql = String.format("TRUNCATE TABLE %s.`%s`",
                        CommonConfig.SinkDatabase, tablesMap.get(tableName));

                statement.execute(sql);
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}

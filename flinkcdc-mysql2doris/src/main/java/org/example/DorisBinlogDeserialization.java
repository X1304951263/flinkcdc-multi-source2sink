package org.example;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.example.common.CommonConfig;
import java.time.LocalDateTime;
import java.util.List;



public class DorisBinlogDeserialization implements DebeziumDeserializationSchema<JSONObject> {

    private static final long serialVersionUID = -316324234222223L;
    public DorisBinlogDeserialization() {

    }


    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<JSONObject> collector) throws Exception {
        String topic = sourceRecord.topic(); //mysql_binlog_source.flinkcdc.user
        String[] dbInfos = topic.split("\\.");
        String dbName = dbInfos[1];
        String tableName = dbInfos[2];

        Struct struct = (Struct) sourceRecord.value();
        Struct before = struct.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if(before != null){
            List<Field> fields = before.schema().fields();
            for(Field f : fields){
                Object v = before.get(f);
                beforeJson.put(f.name(), v);
            }
        }

        Struct after = struct.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if(after != null){
            List<Field> fields = after.schema().fields();
            for(Field f : fields){
                //System.out.println(f.schema());
                Object v = after.get(f);
                afterJson.put(f.name(), v);
            }
        }
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();   //read、update、delete、create

        JSONObject result = new JSONObject();
        result.put("dbName",dbName);
        result.put("tableName",tableName);
        result.put("type",type);

        if(CommonConfig.diyColMap.containsKey(tableName) && !CommonConfig.diyColMap.get(tableName).isEmpty()){

            JSONObject beforeObj = new JSONObject();
            JSONObject afterObj = new JSONObject();

            for(String key : beforeJson.keySet()){
                if(CommonConfig.diyColMap.get(tableName).containsKey(key)){
                    beforeObj.put(CommonConfig.diyColMap.get(tableName).get(key), beforeJson.get(key));
                    continue;
                }
                beforeObj.put(key, beforeJson.get(key));
            }

            beforeObj.put("eventtime", LocalDateTime.now());
            beforeObj.put("__DORIS_DELETE_SIGN__", "1");
            result.put("before",beforeObj);

            for (String key : afterJson.keySet()) {
                if (CommonConfig.diyColMap.get(tableName).containsKey(key)) {
                    afterObj.put(CommonConfig.diyColMap.get(tableName).get(key), afterJson.get(key));
                    continue;
                }
                afterObj.put(key, afterJson.get(key));
            }

            afterObj.put("eventtime", LocalDateTime.now());
            afterObj.put("__DORIS_DELETE_SIGN__", "0");
            result.put("after",afterObj);

        }else{
            beforeJson.put("eventtime", LocalDateTime.now());
            beforeJson.put("__DORIS_DELETE_SIGN__", "1");
            result.put("before",beforeJson);

            afterJson.put("eventtime", LocalDateTime.now());
            afterJson.put("__DORIS_DELETE_SIGN__", "0");
            result.put("after",afterJson);
        }
        //发送数据至下游
        //Thread.sleep(1000);
        //System.out.println(result.toString());
        collector.collect(result);
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return BasicTypeInfo.of(JSONObject.class);
    }



}

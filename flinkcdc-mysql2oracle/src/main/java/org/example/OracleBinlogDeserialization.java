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

import java.util.List;


public class OracleBinlogDeserialization implements DebeziumDeserializationSchema<JSONObject> {

    private static final long serialVersionUID = -316324234222223L;

    public OracleBinlogDeserialization() {

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
        if (before != null) {
            List<Field> fields = before.schema().fields();
            for (Field f : fields) {
                Object v = before.get(f);
                beforeJson.put(f.name(), v);
            }
        }

        Struct after = struct.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null) {
            List<Field> fields = after.schema().fields();
            for (Field f : fields) {
                Object v = after.get(f);
                afterJson.put(f.name(), v);
            }
        }
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();   //read、update、delete、create

        JSONObject result = new JSONObject();
        result.put("dbName", dbName);
        result.put("tableName", tableName);
        result.put("type", type);

        result.put("before", beforeJson);
        result.put("after", afterJson);

        result.put("key", "windowKey");  //用于窗口函数分区,全部分到一个区

        //发送数据至下游
        //Thread.sleep(5000);
        //System.out.println(result.toString());
        collector.collect(result);
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return BasicTypeInfo.of(JSONObject.class);
    }


}

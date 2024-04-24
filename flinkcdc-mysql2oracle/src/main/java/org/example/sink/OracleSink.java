package org.example.sink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.Serializable;
import java.util.List;

public class OracleSink extends RichSinkFunction<List<JSONObject>> implements Serializable {

    private static final long serialVersionUID = 132422341L;

    public Output jdbcOutput;

    public OracleSink(Output jdbcOutput) {
        this.jdbcOutput = jdbcOutput;
    }




    public void open(Configuration parameters) throws Exception {
    }

    @Override
    public void invoke(List<JSONObject> value, Context ctx) throws Exception {
        jdbcOutput.writeRecord(value);
    }


    public void close() throws Exception {
    }
}

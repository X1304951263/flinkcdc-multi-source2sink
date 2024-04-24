package org.example.sink;

import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.util.List;

public interface Output {
    void writeRecord(List<JSONObject> value) throws IOException;
}

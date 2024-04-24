package org.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class MyWindowFunction implements AllWindowFunction<JSONObject, List<JSONObject>, GlobalWindow> {
    @Override
    public void apply(GlobalWindow window, Iterable<JSONObject> input, Collector<List<JSONObject>> out) throws Exception {
        // 实现窗口处理逻辑
        //List<JSONObject> res = Lists.newArrayList(input);

        // 创建一个新的 ArrayList 来存储 JSONObject
        List<JSONObject> res = new ArrayList<>();

        // 将 Iterable 中的元素添加到 ArrayList 中
        for (JSONObject obj : input) {
            res.add(obj);
        }

        if(res.size() > 0){
            out.collect(res);
            res.clear();
        }
    }
}
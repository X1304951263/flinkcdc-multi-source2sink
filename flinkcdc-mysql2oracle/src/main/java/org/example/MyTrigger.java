package org.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.LinkedList;
import java.util.Queue;

public class MyTrigger<W extends Window> extends Trigger<JSONObject, W> {
    private final int maxElementCount;  // 窗口最大元素个数
    private final long intervals;
    private final ValueStateDescriptor<Integer> elementCountStateDescriptor;

    private byte[]  lock = new byte[]{};

    private long windowVersion = 0;   //窗口版本

    private Queue<Long> processVersionQue = new LinkedList<>();


    public MyTrigger(int maxElementCount, long intervals) {
        this.maxElementCount = maxElementCount;
        this.intervals = intervals;
        this.elementCountStateDescriptor =
                new ValueStateDescriptor<>("elementCount", Integer.class);

    }

    @Override
    public TriggerResult onElement(JSONObject o, long l, W w, TriggerContext triggerContext) throws Exception {
        synchronized (this.lock){
            ValueState<Integer> elementCountState = triggerContext.getPartitionedState(this.elementCountStateDescriptor);
            //如果遇到更新语句，触发窗口处理
            String type = o.getString("type");
            if("update".equals(type) || "delete".equals(type)){
                elementCountState.clear(); // 清空计数器
                this.windowVersion++;
                return TriggerResult.FIRE_AND_PURGE; // 触发窗口处理并清空窗口数据
            }

            //每一个窗口当第一个元素来的时候，注册一个定时器，intervals毫秒后触发窗口处理
            Integer currentCount = elementCountState.value();
            if (currentCount == null || currentCount == 0) {
                currentCount = 0;
                processVersionQue.add(this.windowVersion);
                long currentTime = triggerContext.getCurrentProcessingTime();
                triggerContext.registerProcessingTimeTimer(currentTime + this.intervals);
            }
            currentCount++;

            // 当前窗口元素数量是否已达上限，达到上限触发窗口处理
            if (currentCount >= this.maxElementCount) {
                elementCountState.clear();
                this.windowVersion++;
                return TriggerResult.FIRE_AND_PURGE;
            } else {
                elementCountState.update(currentCount); // 更新计数器
            }

            return TriggerResult.CONTINUE; // 继续等待
        }
    }

    @Override
    public TriggerResult onProcessingTime(long l, W w, TriggerContext triggerContext) throws Exception {
        synchronized (this.lock){
            long t = -1;
            if(!this.processVersionQue.isEmpty()){
                t = this.processVersionQue.remove();
            }
            if(t != this.windowVersion){
                return TriggerResult.CONTINUE;
            }
            ValueState<Integer> elementCountState = triggerContext.getPartitionedState(this.elementCountStateDescriptor);
            elementCountState.clear(); // 清空计数器
            return TriggerResult.FIRE_AND_PURGE;
        }
    }

    @Override
    public TriggerResult onEventTime(long l, W w, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W w, TriggerContext triggerContext) throws Exception {

    }
}

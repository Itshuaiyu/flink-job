package com.sino.dbass.task.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WindowReduceExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //env.addSource(new Click)
    }
}

package com.itguigu.cp05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformSimpleAggTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> streamSource = env.fromElements(
                new Event("Mary", "/1", 1000L),
                new Event("Bob", "/2", 2000L),
                new Event("Alice", "/3", 3000L),
                new Event("Bob", "/4", 4000L),
                new Event("Bob", "/5", 5000L),
                new Event("Alice", "/6", 6000L),
                new Event("Bob", "/7", 7000L),
                new Event("Bob", "/8", 8000L)
        );

        // 只是timestamp字段最新
        streamSource.keyBy(item -> item.user).max("timestamp").print();
        // 整体上最新
        streamSource.keyBy(item -> item.user).maxBy("timestamp").print();

        env.execute();
    }
}

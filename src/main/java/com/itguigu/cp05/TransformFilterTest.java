package com.itguigu.cp05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformFilterTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> streamSource = env.fromElements(
                new Event("a", "/prod", 4L),
                new Event("b", "/id", 4L),
                new Event("c", "/name", 4L)
        );
        streamSource.filter(item -> item.user.endsWith("c")).print();
        env.execute();
    }

}

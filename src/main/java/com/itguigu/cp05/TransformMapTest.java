package com.itguigu.cp05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformMapTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> streamSource = env.fromElements(
                new Event("a", "/prod", 4L),
                new Event("b", "/id", 4L),
                new Event("c", "/name", 4L)
        );
        // 1. 类
        // SingleOutputStreamOperator<String> map = streamSource.map(new MapFunc());
        // map.print();

        // 2. 匿名函数(略)

        // 3. lambda
        streamSource.map(item -> item.url).print();
        env.execute();
    }

    public static class MapFunc implements MapFunction<Event, String> {
        @Override
        public String map(Event event) throws Exception {
            return event.url;
        }
    }
}

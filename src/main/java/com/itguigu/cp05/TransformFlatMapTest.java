package com.itguigu.cp05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * flatMap扁平映射
 *
 * @author yangxuan
 */
public class TransformFlatMapTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> streamSource = env.fromElements(
                new Event("a", "/prod", 4L),
                new Event("b", "/id", 4L),
                new Event("c", "/name", 4L)
        );
        // streamSource.flatMap(new MyFlatMap()).print();

        streamSource.flatMap((Event event, Collector<String> out) -> {
            out.collect(event.url);
            out.collect(event.user);
        })// .returns(new TypeHint<String>() { })
                .returns(String.class)
                .print();
        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<Event, String> {

        @Override
        public void flatMap(Event event, Collector<String> out) throws Exception {
            out.collect(event.url);
            out.collect(event.user);
            out.collect(event.timestamp.toString());
        }
    }


}

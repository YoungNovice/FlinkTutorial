package com.itguigu.cp05;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformReduceTest {

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

        // 统计用户点击次数
        SingleOutputStreamOperator<Tuple2<String, Long>> clicks = streamSource.map(item -> Tuple2.of(item.user, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(item -> item.f0)
                .reduce((ReduceFunction<Tuple2<String, Long>>) (value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        SingleOutputStreamOperator<Tuple2<String, Long>> result = clicks.keyBy(item -> "key")
                .reduce((ReduceFunction<Tuple2<String, Long>>) ((value1, value2) -> (value1.f1 > value2.f1) ? value1 : value2))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        result.print();

        env.execute();
    }
}

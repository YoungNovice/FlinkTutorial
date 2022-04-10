package com.itguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class BoundedStreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.readTextFile("input/words.txt");
        // 转换
        SingleOutputStreamOperator<Tuple2<String, Long>> wordTuple = streamSource
                .flatMap(BoundedStreamWordCount::ofTuple2)
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 分组
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordTuple.keyBy(data -> data.f0);
        // 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);
        // 打印
        sum.print();
        // 执行
        env.execute();
    }

    public static void ofTuple2(String line, Collector<Tuple2<String, Long>> collector) {
        String[] words = line.split(" ");
        for (String word : words) {
            collector.collect(Tuple2.of(word, 1L));
        }
    }
}

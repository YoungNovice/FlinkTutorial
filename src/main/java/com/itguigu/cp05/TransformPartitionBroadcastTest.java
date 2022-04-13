package com.itguigu.cp05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class TransformPartitionBroadcastTest {

        public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            DataStreamSource<Event> streamSource = env.fromElements(
                    new Event("a", "/prod", 4L),
                    new Event("b", "/id", 4L),
                    new Event("c", "/name", 4L)
            );

            streamSource.broadcast().print().setParallelism(4);
            env.execute();
        }
}

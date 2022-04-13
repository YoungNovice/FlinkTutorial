package com.itguigu.cp05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformPartitionGlobalTest {

        public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            DataStreamSource<Event> streamSource = env.fromElements(
                    new Event("a", "/prod", 4L),
                    new Event("b", "/id", 4L),
                    new Event("c", "/name", 4L)
            );

            // 5. 全局分区(下游集中到一个子任务)
            // streamSource.global().print().setParallelism(4);

            // 6. 自定义重分区

            env.execute();
        }
}

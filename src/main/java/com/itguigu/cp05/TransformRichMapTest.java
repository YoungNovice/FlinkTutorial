package com.itguigu.cp05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformRichMapTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> streamSource = env.fromElements(
                new Event("a", "/prod", 4L),
                new Event("b", "/id", 4L),
                new Event("c", "/name", 4L)
        );

        streamSource.map(new MyRichFunc()).setParallelism(2)
                .print();

        // streamSource.shuffle().rebalance().rescale();
        env.execute();
    }

    public static class MyRichFunc extends RichMapFunction<Event, String> {

        @Override
        public String map(Event event) throws Exception {
            return event.url.length() + "长度";
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open生命周期" + getRuntimeContext().getIndexOfThisSubtask() + "号任务");
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close生命周期" + getRuntimeContext().getIndexOfThisSubtask() + "号任务");
        }
    }

}

package com.itguigu.cp05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class SourceCustomTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // DataStreamSource<Event> streamSource = env.addSource(new ClickSource());
        // streamSource.print();

        env.setParallelism(4);
        DataStreamSource<Integer> intStream = env.addSource(new IntStreamSource()).setParallelism(2);
        intStream.print().setParallelism(1);
        env.execute();
    }

    public static class IntStreamSource implements ParallelSourceFunction<Integer> {

        private volatile Boolean running = true;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                ctx.collect(new Random().nextInt());
                TimeUnit.MILLISECONDS.sleep(200);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}

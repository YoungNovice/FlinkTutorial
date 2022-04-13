package com.itguigu.cp05.sink;

import com.itguigu.cp05.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class SinkToFileTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

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
        StreamingFileSink<String> fileSink = StreamingFileSink.forRowFormat(
                        new Path("./output"),
                        new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                // 大小
                                .withMaxPartSize(1024)
                                // 时间
                                .withRolloverInterval(TimeUnit.MINUTES.toMinutes(15))
                                // 不活跃
                                .withInactivityInterval(TimeUnit.MINUTES.toMinutes(5))
                                .build()
                )
                .build();
        streamSource.map(Event::toString).addSink(fileSink);
        env.execute();

    }
}

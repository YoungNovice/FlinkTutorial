package com.itguigu.cp05;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class SourceTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从文件
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");
        // 2. 从集合
        List<Event> events = Lists.newArrayList();
        events.add(new Event("1", "2", 3L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);
        // 3. 从元素
        DataStreamSource<Event> stream3 = env.fromElements(new Event("2", "3", 4L));
        // 4. 从socket
        // DataStreamSource<String> stream4 = env.socketTextStream("CentOS2", 8888);

        stream1.print("1");
        stream2.print("2");
        stream3.print("3");
        env.execute();
    }
}

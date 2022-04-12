package com.itguigu.cp05;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.List;
import java.util.Properties;

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
        // stream1.print("1");
        // stream2.print("2");
        // stream3.print("3");

        // 从kafka中读取
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "CentOS1:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("clicks", new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> kafkaStream = env.addSource(kafkaConsumer);

        kafkaStream.print("kafka");
        env.execute();
    }
}

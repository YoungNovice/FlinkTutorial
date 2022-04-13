package com.itguigu.cp05.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * zk
 * ./bin/kafka-server-start.sh -daemon ./config/server.properties
 * kafka
 * ./bin/kafka-server-start.sh -daemon ./config/server.properties
 *
 * 生产者
 * ./bin/kafka-console-producer.sh --broker-list CentOS1:9092 --topic clicks
 * 消费者
 * ./bin/kafka-console-consumer.sh --bootstrap-server CentOS1:9092 --topic events
 */
public class SinkToKafkaTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 1. 从kafka中读取
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "CentOS1:9092");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("clicks", new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> stream = env.addSource(kafkaConsumer);

        // 2. 转换
        SingleOutputStreamOperator<String> result = stream.map(item -> "<" + item + ">");

        // 3. 写入kafka
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("CentOS1:9092", "events", new SimpleStringSchema());
        result.addSink(kafkaProducer);

        env.execute();
    }
}

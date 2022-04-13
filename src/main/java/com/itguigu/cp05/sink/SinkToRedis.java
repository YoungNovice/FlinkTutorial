package com.itguigu.cp05.sink;

import com.itguigu.cp05.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class SinkToRedis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("127.0.0.1").setPort(6379)
                .build();

        streamSource.addSink(new RedisSink<>(config, new MyRedisMapper()));
        env.execute();
    }

    public static class MyRedisMapper implements RedisMapper<Event> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "clicks");
        }

        @Override
        public String getKeyFromData(Event event) {
            return event.user;
        }

        @Override
        public String getValueFromData(Event event) {
            return event.toString();
        }
    }
}

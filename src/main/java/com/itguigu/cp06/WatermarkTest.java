package com.itguigu.cp06;

import com.itguigu.cp05.Event;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;

public class WatermarkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.getConfig().setAutoWatermarkInterval(200L);

        DataStream<Event> stream = env.fromElements(
                        new Event("Mary", "/1", 1000L),
                        new Event("Bob", "/2", 2000L),
                        new Event("Alice", "/3", 3000L),
                        new Event("Bob", "/4", 4000L),
                        new Event("Bob", "/5", 5000L),
                        new Event("Alice", "/6", 6000L),
                        new Event("Bob", "/7", 7000L),
                        new Event("Bob", "/8", 8000L)
                )
                // 有序流的watermark生成
                /*.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.timestamp;
                                    }
                                })
                )*/
                // 乱序流的watermark生成
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.timestamp;
                                    }
                                })
                );

        env.execute();
    }

    public static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomBoundedOutOfOrdernessGenerator();
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event event, long l) {
                    return event.timestamp; // 告诉程序时间戳是哪个字段
                }
            };
        }
    }

    /**
     * 周期生成
     */
    public static class CustomBoundedOutOfOrdernessGenerator implements WatermarkGenerator<Event> {

        // 延迟时间
        private final Long delayTime = 5000L;
        // 观察到的最大时间戳
        private Long maxTs = Long.MIN_VALUE + delayTime + 1L;

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput watermarkOutput) {
            // 来一条数据调用一次 更新最大时间戳
            maxTs = Math.max(event.timestamp, maxTs);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput out) {
            // 发射水位线 默认200ms调用一次
            out.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }

    /**
     * 断点式生成
     */
    public static class PunctuatedGenerator implements WatermarkGenerator<Event> {
        @Override
        public void onEvent(Event event, long l, WatermarkOutput out) {
            if (event.user.contains("Mary")) {
                out.emitWatermark(new Watermark(event.timestamp - 1));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

        }
    }
}

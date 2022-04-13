package com.itguigu.cp05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class TransformPartitionRescaleTest {

        public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            env.addSource(new RichParallelSourceFunction<Integer>() {

                @Override
                public void run(SourceContext<Integer> ctx) throws Exception {
                    // 这个run执行了多次 取决于并行度
                    for (int i = 1; i <= 8; i++) {
                        if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                            ctx.collect(i);
                        } else {
                            System.out.println("i = " + i + " index = " + getRuntimeContext().getIndexOfThisSubtask());
                        }
                    }
                }

                @Override
                public void cancel() {

                }
            }).setParallelism(3).rescale().print().setParallelism(4);
            env.execute();
        }
}

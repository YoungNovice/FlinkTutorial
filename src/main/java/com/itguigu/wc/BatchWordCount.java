package com.itguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;

public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 读取数据
        DataSource<String> dataSource = env.readTextFile("input/words.txt");
        // 分词
        FlatMapOperator<String, Tuple2<String, Long>> wordTuple = dataSource.flatMap(Util::of)
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 分组
        UnsortedGrouping<Tuple2<String, Long>> wordGroup = wordTuple.groupBy(0);
        // 统计
        AggregateOperator<Tuple2<String, Long>> wordSum = wordGroup.sum(1);
        // 打印
        wordSum.print();
    }

}

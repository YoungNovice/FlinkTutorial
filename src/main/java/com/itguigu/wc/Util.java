package com.itguigu.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public final class Util {

    public static void of(String line, Collector<Tuple2<String, Long>> collector) {
        String[] words = line.split(" ");
        for (String word : words) {
            collector.collect(Tuple2.of(word, 1L));
        }
    }
}

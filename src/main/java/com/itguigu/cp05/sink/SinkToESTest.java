package com.itguigu.cp05.sink;

import com.itguigu.cp05.Event;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.List;

/**
 * http://localhost:9201/_cat/indices?v
 * http://localhost:9201/clicks/_search?pretty
 */
public class SinkToESTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

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


        List<HttpHost> hosts = Lists.newArrayList();
        hosts.add(new HttpHost("127.0.0.1", 9201));

        ElasticsearchSinkFunction<Event> sinkFunction = new ElasticsearchSinkFunction<Event>() {
            @Override
            public void process(Event event, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                HashMap<String, String> map = new HashMap<>();
                map.put(event.user, event.url);

                IndexRequest request = Requests.indexRequest()
                        .index("clicks")
                        .type("type")
                        .source(map);
                requestIndexer.add(request);
            }
        };
        streamSource.addSink(new ElasticsearchSink.Builder<>(hosts, sinkFunction).build());
        env.execute();
    }
}

package com.itguigu.cp05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();
        String[] users = {"a", "b", "c"};
        String[] urls = {"./home", "./cart?id=1", "./prod"};
        while (running) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            ctx.collect(new Event(user, url, Calendar.getInstance().getTimeInMillis()));
            Thread.sleep(500L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

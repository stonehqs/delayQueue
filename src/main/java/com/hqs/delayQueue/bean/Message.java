package com.hqs.delayQueue.bean;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * @author huangqingshi
 * @Date 2020-04-18
 */
public class Message implements Delayed {

    private String body;
    private long fireTime;

    public String getBody() {
        return body;
    }

    public long getFireTime() {
        return fireTime;
    }

    public Message(String body, long delayTime) {
        this.body = body;
        this.fireTime = delayTime + System.currentTimeMillis();
    }

    public long getDelay(TimeUnit unit) {

        return unit.convert(this.fireTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    public int compareTo(Delayed o) {
        return (int) (this.getDelay(TimeUnit.MILLISECONDS) -o.getDelay(TimeUnit.MILLISECONDS));
    }

    @Override
    public String toString() {
        return System.currentTimeMillis() + ":" + body;
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println(System.currentTimeMillis() + ":start");
        BlockingQueue<Message> queue = new DelayQueue<>();
        Message message1 = new Message("hello", 1000 * 5L);
        Message message2 = new Message("world", 1000 * 7L);

        queue.put(message1);
        queue.put(message2);

        while (queue.size() > 0) {
            System.out.println(queue.take());
        }
    }
}


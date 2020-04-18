package com.hqs.delayQueue.cache;

import com.hqs.delayQueue.bean.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.BlockingQueue;

/**
 * @author huangqingshi
 * @Date 2020-04-18
 */
@Slf4j
public class RedisListDelayedQueue{

    private static final int MAX_SIZE_OF_QUEUE = 100000;
    private RedisTemplate<String, String> redisTemplate;
    private String queueName;
    private BlockingQueue<Message> delayedQueue;

    public RedisListDelayedQueue(RedisTemplate<String, String> redisTemplate, String queueName, BlockingQueue<Message> delayedQueue) {
        this.redisTemplate = redisTemplate;
        this.queueName = queueName;
        this.delayedQueue = delayedQueue;
        init();
    }

    public void offerMessage(Message message) {
        if(delayedQueue.size() > MAX_SIZE_OF_QUEUE) {
            throw new IllegalStateException("超过队列要求最大值，请检查");
        }
        try {
            log.info("offerMessage:" + message);
            delayedQueue.offer(message);
        } catch (Exception e) {
            log.error("offMessage异常", e);
        }
    }

    public void init() {
        new Thread(() -> {
            while(true) {
                try {
                    Message message = delayedQueue.take();
                    redisTemplate.opsForList().leftPush(queueName, message.toString());
                } catch (InterruptedException e) {
                    log.error("取消息错误", e);
                }
            }
        }).start();
    }
}

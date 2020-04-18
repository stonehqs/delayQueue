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
public class RedisZSetDelayedQueue {

    private static final int MAX_SIZE_OF_QUEUE = 100000;
    private RedisTemplate<String, String> redisTemplate;
    private String queueName;
    private BlockingQueue<Message> delayedQueue;

    public RedisZSetDelayedQueue(RedisTemplate<String, String> redisTemplate, String queueName, BlockingQueue<Message> delayedQueue) {
        this.redisTemplate = redisTemplate;
        this.queueName = queueName;
        this.delayedQueue = delayedQueue;
    }

    public void offerMessage(Message message) {
        if(delayedQueue.size() > MAX_SIZE_OF_QUEUE) {
            throw new IllegalStateException("超过队列要求最大值，请检查");
        }
        long delayTime = message.getFireTime() - System.currentTimeMillis();
        log.info("zset offerMessage" + message + delayTime);
        redisTemplate.opsForZSet().add(queueName, message.toString(), message.getFireTime());
    }

}

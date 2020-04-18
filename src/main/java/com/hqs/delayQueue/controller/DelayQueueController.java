package com.hqs.delayQueue.controller;

import com.hqs.delayQueue.bean.Message;
import com.hqs.delayQueue.cache.RedisListDelayedQueue;
import com.hqs.delayQueue.cache.RedisZSetDelayedQueue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Set;
import java.util.concurrent.*;

/**
 * @author huangqingshi
 * @Date 2020-04-18
 */
@Slf4j
@Controller
public class DelayQueueController {

    private static final int CORE_SIZE = Runtime.getRuntime().availableProcessors();

    //注意RedisTemplate用的String,String，后续所有用到的key和value都是String的
    @Autowired
    RedisTemplate<String, String> redisTemplate;

    private static ThreadPoolExecutor taskExecPool = new ThreadPoolExecutor(CORE_SIZE, CORE_SIZE, 0, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>());

    @GetMapping("/redisTest")
    @ResponseBody
    public String redisTest() {
        redisTemplate.opsForValue().set("a","b",60L, TimeUnit.SECONDS);
        System.out.println(redisTemplate.opsForValue().get("a"));
        return "s";
    }

    @GetMapping("/redis/listDelayedQueue")
    @ResponseBody
    public String listDelayedQueue() {

        Message message1 = new Message("hello", 1000 * 5L);
        Message message2 = new Message("world", 1000 * 7L);

        String queueName = "list_queue";

        BlockingQueue<Message> delayedQueue = new DelayQueue<>();

        RedisListDelayedQueue redisListDelayedQueue = new RedisListDelayedQueue(redisTemplate, queueName, delayedQueue);

        redisListDelayedQueue.offerMessage(message1);
        redisListDelayedQueue.offerMessage(message2);
        asyncListTask(queueName);

        return "success";
    }

    @GetMapping("/redis/zSetDelayedQueue")
    @ResponseBody
    public String zSetDelayedQueue() {

        Message message1 = new Message("hello", 1000 * 5L);
        Message message2 = new Message("world", 1000 * 7L);

        String queueName = "zset_queue";

        BlockingQueue<Message> delayedQueue = new DelayQueue<>();

        RedisZSetDelayedQueue redisZSetDelayedQueue = new RedisZSetDelayedQueue(redisTemplate, queueName, delayedQueue);

        redisZSetDelayedQueue.offerMessage(message1);
        redisZSetDelayedQueue.offerMessage(message2);
        asyncZSetTask(queueName);

        return "success";
    }

    public void asyncListTask(String queueName) {
        taskExecPool.execute(() -> {
            for(;;) {
                String message = redisTemplate.opsForList().rightPop(queueName);
                if(message != null) {
                    log.info(message);
                }
            }
        });
    }

    public void asyncZSetTask(String queueName) {
        taskExecPool.execute(() -> {
            for(;;) {
                Long nowTimeInMs = System.currentTimeMillis();
                System.out.println("nowTimeInMs:" + nowTimeInMs);
                Set<String> messages = redisTemplate.opsForZSet().rangeByScore(queueName, 0, nowTimeInMs);
                if(messages != null && messages.size() != 0) {
                    redisTemplate.opsForZSet().removeRangeByScore(queueName, 0, nowTimeInMs);
                    for (String message : messages) {
                        log.info("asyncZSetTask:" + message + " " + nowTimeInMs);
                    }
                    log.info(redisTemplate.opsForZSet().zCard(queueName).toString());
                }
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

}

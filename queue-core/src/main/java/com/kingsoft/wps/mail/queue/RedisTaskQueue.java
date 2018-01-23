package com.kingsoft.wps.mail.queue;

import com.alibaba.fastjson.JSON;
import com.kingsoft.wps.mail.distributed.lock.DistributedLock;
import com.kingsoft.wps.mail.queue.config.Constant;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.logging.Logger;

/**
 * Created by 刘春龙 on 2017/3/3.
 * <p>
 * 任务队列Redis实现<br/>
 */
public class RedisTaskQueue extends TaskQueue {

    private static final Logger logger = Logger.getLogger(RedisTaskQueue.class.getName());

    private static final int REDIS_DB_IDX = 0;

    /**
     * 任务队列名称
     */
    private final String name;

    /**
     * 队列模式：DEFAULT - 简单队列，SAFE - 安全队列
     */
    private final String mode;

    /**
     * 队列管理器
     */
    private KMQueueAdapter kmQueueAdapter;

    /**
     * 构造函数
     *
     * @param kmQueueAdapter 队列管理器
     * @param name           任务队列名称
     * @param mode           队列模式
     */
    public RedisTaskQueue(KMQueueAdapter kmQueueAdapter, String name, String mode) {
        this.kmQueueAdapter = kmQueueAdapter;
        if (mode == null || "".equals(mode)) {
            mode = KMQueueManager.DEFAULT;
        }
        this.name = name;
        this.mode = mode;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getMode() {
        return this.mode;
    }

    /**
     * 向任务队列中插入任务
     * <p>
     * 如果插入任务成功，则返回该任务，失败，则返回null
     * <p>
     * 特别的，对于唯一性任务，如果该任务在队列已经存在，则返回null
     *
     * @param task 队列任务
     * @return 插入的任务
     */
    @Override
    public Task pushTask(Task task) {
        Jedis jedis = null;
        try {
            jedis = kmQueueAdapter.getResource(REDIS_DB_IDX);

            // 队列任务唯一性校验
            if (this.getMode().equals(KMQueueAdapter.SAFE) && task.isUnique()) {// 唯一性任务

                // Integer reply, specifically: 1 if the new element was added 0 if the element was already a member of the set
                Long isExist = jedis.sadd(this.name + Constant.UNIQUE_SUFFIX, task.getId());
                if (isExist == 0) {
                    return null;
                }
            }

            String taskJson = JSON.toJSONString(task);
            jedis.lpush(this.name, taskJson);
            return task;
        } catch (Throwable e) {
            logger.info(e.getMessage());
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                kmQueueAdapter.returnResource(jedis);
            }
        }
        return null;
    }

    @Override
    public void pushTaskToHeader(Task task) {

        Jedis jedis = null;
        try {
            jedis = kmQueueAdapter.getResource(REDIS_DB_IDX);
            String taskJson = JSON.toJSONString(task);
            jedis.rpush(this.name, taskJson);
        } catch (Throwable e) {
            logger.info(e.getMessage());
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                kmQueueAdapter.returnResource(jedis);
            }
        }

    }

    /**
     * 1.采用阻塞队列，以阻塞的方式(brpop)获取任务队列中的任务；<br>
     * 2.判断任务存活时间是否超时（对应的是大于`aliveTimeout`）；<br>
     * 3.更新任务的执行时间戳，放入备份队列的队首；<br>
     * <p>
     * 任务状态不变，默认值为`normal`
     *
     * @return
     */
    @Override
    public Task popTask() {
        Jedis jedis = null;
        Task task = null;
        try {
            jedis = kmQueueAdapter.getResource(REDIS_DB_IDX);

            // 判断队列模式
            if (KMQueueManager.SAFE.equals(getMode())) {// 安全队列
                // 1.采用阻塞队列，获取任务队列中的任务(brpop)；
                List<String> result = jedis.brpop(0, getName());
                task = JSON.parseObject(result.get(1), Task.class);

                // 2.判断任务存活时间是否超时（对应的是大于`aliveTimeout`）；
                Task.TaskStatus status = task.getTaskStatus();// 获取任务状态
                long taskGenTimeMillis = status.getGenTimestamp();// 任务生成的时间戳
                long currentTimeMillis = System.currentTimeMillis();// 当前时间戳
                long intervalTimeMillis = currentTimeMillis - taskGenTimeMillis;// 任务的存活时间
                if (intervalTimeMillis <= kmQueueAdapter.getAliveTimeout()) {// 如果大于存活超时时间，则不再执行
                    // 3.更新任务的执行时间戳，放入备份队列的队首；
                    task.getTaskStatus().setExcTimestamp(System.currentTimeMillis());// 更新任务的执行时间戳
                    jedis.lpush(kmQueueAdapter.getBackUpQueueName(), JSON.toJSONString(task));
                }
            } else if (KMQueueManager.DEFAULT.equals(getMode())) {// 简单队列
                List<String> result = jedis.brpop(0, getName());
                String taskJson = result.get(1);
                task = JSON.parseObject(taskJson, Task.class);
            }
        } catch (Throwable e) {
            logger.info(e.getMessage());
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                kmQueueAdapter.returnResource(jedis);
            }
        }
        return task;
    }

    @Override
    public void finishTask(Task task) {
        if (KMQueueManager.SAFE.equals(getMode())) {
            // 安全队列
            Jedis jedis = null;
            try {
                jedis = kmQueueAdapter.getResource(REDIS_DB_IDX);
                String taskJson = JSON.toJSONString(task);

                // 删除备份队列中的任务
                jedis.lrem(kmQueueAdapter.getBackUpQueueName(), 0, taskJson);

                // 删除该任务的存在标记
                jedis.srem(this.name + Constant.UNIQUE_SUFFIX, task.getId());
            } catch (Throwable e) {
                logger.info(e.getMessage());
                e.printStackTrace();
            } finally {
                if (jedis != null) {
                    kmQueueAdapter.returnResource(jedis);
                }
            }
        }
    }

}

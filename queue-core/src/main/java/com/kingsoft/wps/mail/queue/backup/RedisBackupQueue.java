package com.kingsoft.wps.mail.queue.backup;

import com.alibaba.fastjson.JSON;
import com.kingsoft.wps.mail.queue.KMQueueAdapter;
import com.kingsoft.wps.mail.queue.Task;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.logging.Logger;

/**
 * Created by 刘春龙 on 2017/3/5.
 * <p>
 * 备份队列
 */
public class RedisBackupQueue extends BackupQueue {

    private static final Logger logger = Logger.getLogger(RedisBackupQueue.class.getName());

    private static final int REDIS_DB_IDX = 0;
    public static final String MARKER = "marker";

    /**
     * 备份队列的名称
     */
    private final String name;

    /**
     * 队列管理器
     */
    private KMQueueAdapter kmQueueAdapter;

    public RedisBackupQueue(KMQueueAdapter kmQueueAdapter) {
        this.kmQueueAdapter = kmQueueAdapter;
        this.name = kmQueueAdapter.getBackUpQueueName();
    }

    /**
     * 初始化备份队列，添加备份队列循环标记
     */
    @Override
    public void initQueue() {
        Jedis jedis = null;
        try {
            jedis = kmQueueAdapter.getResource(REDIS_DB_IDX);

            // 创建备份队列循环标记
            Task.TaskStatus state = new Task.TaskStatus();
            Task task = new Task(this.name, null, RedisBackupQueue.MARKER, null, state);

            String taskJson = JSON.toJSONString(task);

            // 注意分布式问题，防止备份队列添加多个循环标记
            // 这里使用redis的事务&乐观锁
            jedis.watch(this.name);// 监视当前队列
            boolean isExists = jedis.exists(this.name);// 查询当前队列是否存在

            List<String> backQueueData = jedis.lrange(this.name, 0, -1);
            logger.info("========================================");
            logger.info("Backup queue already exists! Queue name：" + this.name);
            logger.info("Backup queue[" + this.name + "]data:");
            backQueueData.forEach(logger::info);
            logger.info("========================================");

            Transaction multi = jedis.multi();// 开启事务
            if (!isExists) {// 只有当前队列不存在，才执行lpush
                multi.lpush(this.name, taskJson);
                List<Object> results = multi.exec();
                logger.info("Thread[" + Thread.currentThread().getName() + "] - (Add backup queue loop tag) Transaction execution result：" + ((results != null && results.size() > 0) ? results.get(0) : "Fail"));
            }
        } catch (Throwable e) {
            logger.info(e.getMessage());
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                kmQueueAdapter.returnResource(jedis);
            }
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Task popTask() {
        Jedis jedis = null;
        Task task = null;
        try {
            jedis = kmQueueAdapter.getResource(REDIS_DB_IDX);

            /**
             * 循环取出备份队列的一个元素：从队尾取出元素，并将其放置队首
             */
            String taskValue = jedis.rpoplpush(this.name, this.name);
            task = JSON.parseObject(taskValue, Task.class);
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
        Jedis jedis = null;
        try {
            jedis = kmQueueAdapter.getResource(REDIS_DB_IDX);
            String taskJson = JSON.toJSONString(task);
            jedis.lrem(this.name, 0, taskJson);
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

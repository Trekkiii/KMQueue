package com.kingsoft.wps.mail.queue;

import com.kingsoft.wps.mail.utils.Assert;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

/**
 * Created by 刘春龙 on 2018/1/19.
 */
public abstract class KMQueueAdapter {

    // 队列模式：DEFAULT - 简单队列，SAFE - 安全队列
    public static final String DEFAULT = "default";
    public static final String SAFE = "safe";
    public static String BACK_UP_QUEUE_PREFIX = "back_up_queue_";// 备份队列名称前缀

    /**
     * 备份队列名称
     */
    protected String backUpQueueName;

    /**
     * redis连接池
     */
    protected Pool<Jedis> pool;

    /**
     * 获取备份队列的名称
     *
     * @return 备份队列的名称
     */
    public String getBackUpQueueName() {
        return this.backUpQueueName;
    }

    public abstract long getAliveTimeout();

    /**
     * 获取Jedis对象
     * <p>
     * 使用完成后，必须归还到连接池中
     *
     * @return Jedis对象
     */
    public synchronized Jedis getResource() {
        Jedis jedis = this.pool.getResource();
        Assert.notNull(jedis, "Get jedis client failed");
        return jedis;
    }

    /**
     * 获取Jedis对象
     * <p>
     * 使用完成后，必须归还到连接池中
     *
     * @param db Redis数据库序号
     * @return Jedis对象
     */
    public synchronized Jedis getResource(int db) {
        Jedis jedis = this.pool.getResource();
        Assert.notNull(jedis, "Get jedis client failed");
        jedis.select(db);
        return jedis;
    }

    /**
     * 归还Redis连接到连接池
     *
     * @param jedis Jedis对象
     */
    public synchronized void returnResource(Jedis jedis) {
        if (jedis != null) {
//            pool.returnResource(jedis);
            // from Jedis 3.0
            jedis.close();
        }
    }

    public synchronized void destroy() throws Exception {
        pool.destroy();
    }
}

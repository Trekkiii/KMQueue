package com.kingsoft.wps.mail.queue;

import com.kingsoft.wps.mail.exception.NestedException;
import com.kingsoft.wps.mail.queue.backup.BackupQueue;
import com.kingsoft.wps.mail.queue.backup.RedisBackupQueue;
import com.kingsoft.wps.mail.utils.Assert;
import com.kingsoft.wps.mail.utils.KMQUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;
import sun.misc.BASE64Encoder;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Created by 刘春龙 on 2018/1/17.
 * <p>
 * 队列管理器，redis线程池的初始化，队列的初始化
 */
public class KMQueueManager extends KMQueueAdapter {

    private final static Logger logger = Logger.getLogger(KMQueueManager.class.getName());

    private Map<String, Object> queueMap = new ConcurrentHashMap<>();

    /**
     * 待创建的队列的名称集合
     */
    private List<String> queues;

    /**
     * 任务的存活超时时间。单位：ms
     * <p>
     * 注意，该时间是任务从创建({@code new Task(...)})到销毁的总时间
     * <p>
     * 该值只针对安全队列起作用
     * <p>
     * 不设置默认为 Long.MAX_VALUE
     */
    private long aliveTimeout;

    /**
     * 构造方法私有化，防止外部调用
     */
    private KMQueueManager() {
    }

    /**
     * 根据名称获取任务队列
     *
     * @param name 队列名称
     * @return 任务队列
     */
    public TaskQueue getTaskQueue(String name) {
        Object queue = this.queueMap.get(name);
        if (queue != null && queue instanceof TaskQueue) {
            return (TaskQueue) queue;
        }
        return null;
    }

    /**
     * 获取任务存活超时时间。注意，该时间是任务从创建({@code new Task(...)})到销毁的总时间。单位：ms
     *
     * @return
     */
    public long getAliveTimeout() {
        return this.aliveTimeout;
    }

    /**
     * 初始化队列
     */
    public void init() {
        // 生成备份队列名称
        backUpQueueName = KMQUtils.genBackUpQueueName(this.queues);

        logger.info("Initializing the queues");

        boolean hasSq = false;

        for (String queue : this.queues) {
            String[] qInfos = queue.trim().split(":");
            String qName = qInfos[0].trim();// 队列名称
            String qMode = null;// 队列模式
            if (qInfos.length == 2) {
                qMode = qInfos[1].trim();
            }

            if (qMode != null && !"".equals(qMode) && !qMode.equals(DEFAULT) && !qMode.equals(SAFE)) {
                throw new NestedException("The current queue mode is invalid, the queue name：" + qName);
            }

            if (!"".equals(qName)) {
                if (!queueMap.containsKey(qName)) {
                    if (qMode != null && qMode.equals(SAFE)) {
                        hasSq = true;// 标记存在安全队列
                    }

                    queueMap.put(qName, new RedisTaskQueue(this, qName, qMode));
                    logger.info("Creating a task queue：" + qName);
                } else {
                    logger.info("The current queue already exists. Do not create the queue name repeatedly：" + qName);
                }
            } else {
                throw new NestedException("The current queue name is empty!");
            }
        }

        // 添加备份队列
        if (hasSq) {
            BackupQueue backupQueue = new RedisBackupQueue(this);
            backupQueue.initQueue();
            queueMap.put(backUpQueueName, backupQueue);
            logger.info("Initializing backup queue");
        }
    }

    /**
     * 构建器，用于设置初始化参数，执行初始化操作
     */
    public static class Builder {

        /**
         * redis连接方式:
         * <ul>
         * <li>default</li>
         * <li>single</li>
         * <li>sentinel</li>
         * </ul>
         */
        private final String REDIS_CONN_DEFAULT = "default";
        private final String REDIS_CONN_SINGLE = "single";
        private final String REDIS_CONN_SENTINEL = "sentinel";
        private String REDIS_CONN_MODE;

        /**
         * redis连接池
         */
        private Pool<Jedis> pool;

        /**
         * 待创建的队列的名称集合
         */
        private List<String> queues;

        /**
         * redis host
         */
        private String host;

        /**
         * redis port
         */
        private int port;

        /**
         * 主从复制集
         */
        private Set<String> sentinels;

        /**
         * 连接池最大分配的连接数
         */
        private Integer poolMaxTotal;

        /**
         * 连接池的最大空闲连接数
         */
        private Integer poolMaxIdle;

        /**
         * redis获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted)，如果超时就抛异常，小于零:阻塞不确定的时间，默认-1
         */
        private Long poolMaxWaitMillis;

        /**
         * 任务的存活超时时间。注意，该时间是任务从创建({@code new Task(...)})到销毁的总时间。单位：ms
         * <p>
         * 该值只针对安全队列起作用
         * <p>
         * 不设置默认为 Long.MAX_VALUE
         */
        private long aliveTimeout;

        /**
         * 创建Builder对象
         * <p>
         * 使用指定的redis连接池
         *
         * @param pool   redis连接池
         * @param queues 所要创建的队列名称，可以传多个
         */
        public Builder(Pool<Jedis> pool, String... queues) {
            Assert.notNull(pool, "Param pool can't null");

            this.aliveTimeout = Long.MAX_VALUE;
            this.pool = pool;
            this.queues = Arrays.asList(queues);
            this.REDIS_CONN_MODE = this.REDIS_CONN_DEFAULT;
        }

        /**
         * 创建Builder对象
         *
         * @param host   redis host
         * @param port   redis port
         * @param queues 所要创建的队列名称，可以传多个
         */
        public Builder(String host, int port, String... queues) {
            Assert.notNull(host, "Param host can't null");
            Assert.notNull(port, "Param port can't null");

            this.aliveTimeout = Long.MAX_VALUE;
            this.host = host;
            this.port = port;
            this.queues = Arrays.asList(queues);
            this.REDIS_CONN_MODE = this.REDIS_CONN_SINGLE;
        }

        /**
         * 创建Builder对象
         * <p>
         * 采用主从复制的方式创建redis连接池
         *
         * @param hostPort   逗号分隔的 host:port 列表
         * @param isSentinel 是否是主从复制
         * @param queues     所要创建的队列名称，可以传多个
         */
        public Builder(String hostPort, boolean isSentinel, String... queues) {
            Assert.isTrue(isSentinel, "Param isSentinel invalid");
            Assert.notNull(hostPort, "Param hostPort can't null");

            this.aliveTimeout = Long.MAX_VALUE;
            this.sentinels = new HashSet<>();
            sentinels.addAll(Arrays.asList(hostPort.split(",")));
            this.queues = Arrays.asList(queues);
            this.REDIS_CONN_MODE = this.REDIS_CONN_SENTINEL;
        }

        /**
         * 设置redis连接池最大分配的连接数
         * <p>
         * 对使用{@link #Builder(Pool, String...)}构造的Builder不起作用
         *
         * @param poolMaxTotal 连接池最大分配的连接数
         * @return 返回Builder
         */
        public Builder setMaxTotal(Integer poolMaxTotal) {
            Assert.greaterThanEquals(poolMaxTotal, 0, "Param poolMaxTotal is negative");
            this.poolMaxTotal = poolMaxTotal;
            return this;
        }

        /**
         * 设置redis连接池的最大空闲连接数
         * <p>
         * 对使用{@link #Builder(Pool, String...)}构造的Builder不起作用
         *
         * @param poolMaxIdle 连接池的最大空闲连接数
         * @return 返回Builder
         */
        public Builder setMaxIdle(Integer poolMaxIdle) {
            Assert.greaterThanEquals(poolMaxIdle, 0, "Param poolMaxIdle is negative");
            this.poolMaxIdle = poolMaxIdle;
            return this;
        }

        /**
         * 设置redis获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted)，
         * 如果超时就抛异常，小于零则阻塞不确定的时间，默认-1
         * <p>
         * 对使用{@link #Builder(Pool, String...)}构造的Builder不起作用
         *
         * @param poolMaxWaitMillis 获取连接时的最大等待毫秒数
         * @return 返回Builder
         */
        public Builder setMaxWaitMillis(Long poolMaxWaitMillis) {
            this.poolMaxWaitMillis = poolMaxWaitMillis;
            return this;
        }

        /**
         * 设置任务的存活超时时间，传0 则采用默认值： Long.MAX_VALUE
         *
         * @param aliveTimeout 任务的存活时间
         * @return 返回Builder
         */
        public Builder setAliveTimeout(long aliveTimeout) {
            Assert.greaterThanEquals(aliveTimeout, 0, "Param aliveTimeout is negative");
            if (aliveTimeout == 0) {
                aliveTimeout = Long.MAX_VALUE;
            }
            this.aliveTimeout = aliveTimeout;
            return this;
        }

        public KMQueueManager build() {

            KMQueueManager queueManager = new KMQueueManager();

            JedisPoolConfig jedisPoolConfig = null;
            switch (REDIS_CONN_MODE) {
                case REDIS_CONN_DEFAULT:
                    break;
                case REDIS_CONN_SINGLE:
                    jedisPoolConfig = new JedisPoolConfig();
                    jedisPoolConfig.setMaxTotal(this.poolMaxTotal);
                    jedisPoolConfig.setMaxIdle(this.poolMaxIdle);
                    jedisPoolConfig.setMaxWaitMillis(this.poolMaxWaitMillis);
                    this.pool = new JedisPool(jedisPoolConfig, host, port);
                    break;
                case REDIS_CONN_SENTINEL:
                    jedisPoolConfig = new JedisPoolConfig();
                    jedisPoolConfig.setMaxTotal(this.poolMaxTotal);
                    jedisPoolConfig.setMaxIdle(this.poolMaxIdle);
                    jedisPoolConfig.setMaxWaitMillis(this.poolMaxWaitMillis);
                    this.pool = new JedisSentinelPool("master", sentinels, jedisPoolConfig);
                    break;
            }
            queueManager.pool = this.pool;
            queueManager.queues = this.queues;
            queueManager.aliveTimeout = this.aliveTimeout;
            return queueManager;
        }
    }
}

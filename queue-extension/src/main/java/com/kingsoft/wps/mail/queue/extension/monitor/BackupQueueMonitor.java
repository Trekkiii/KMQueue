package com.kingsoft.wps.mail.queue.extension.monitor;

import com.alibaba.fastjson.JSON;
import com.kingsoft.wps.mail.queue.*;
import com.kingsoft.wps.mail.queue.backup.BackupQueue;
import com.kingsoft.wps.mail.queue.backup.RedisBackupQueue;
import com.kingsoft.wps.mail.queue.config.Constant;
import com.kingsoft.wps.mail.utils.Assert;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Created by 刘春龙 on 2017/3/5.
 * <p>
 * 备份队列监控
 * <p>
 * 超时任务重试
 */
public class BackupQueueMonitor extends KMQueueAdapter {

    private static final Logger logger = Logger.getLogger(BackupQueueMonitor.class.getName());

    /**
     * 任务超时重试次数
     */
    private int retryTimes;

    /**
     * 失败任务（重试三次失败）的处理方式
     */
    private Pipeline pipeline;

    /**
     * 任务的存活超时时间。注意，该时间是任务从创建({@code new Task(...)})到销毁的总时间。单位：ms
     * <p>
     * 该值只针对安全队列起作用
     * <p>
     * 不设置默认为 Long.MAX_VALUE
     */
    private long aliveTimeout;

    /**
     * 任务执行的超时时间（一次执行），单位：ms
     * <p>
     * 该值只针对安全队列起作用
     * <p>
     * 不设置默认为 Long.MAX_VALUE
     */
    private long protectedTimeout;

    /**
     * 健康检查
     */
    private AliveDetectHandler aliveDetectHandler;

    /**
     * 构造方法私有化，防止外部调用
     */
    private BackupQueueMonitor() {
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    /**
     * 任务的存活超时时间。注意，该时间是任务从创建({@code new Task(...)})到销毁的总时间。单位：ms
     * <p>
     * 该值只针对安全队列起作用
     * <p>
     * 不设置默认为 Long.MAX_VALUE
     *
     * @return 任务的存活时间
     */
    public long getAliveTimeout() {
        return aliveTimeout;
    }

    /**
     * 任务执行的超时时间（一次执行），单位：ms
     * <p>
     * 该值只针对安全队列起作用
     * <p>
     * 不设置默认为 Long.MAX_VALUE
     *
     * @return 任务执行的超时时间
     */
    public long getProtectedTimeout() {
        return protectedTimeout;
    }

    /**
     * 启动监控
     */
    public void monitor() {

        BackupQueue backupQueue = null;
        Task task;

        try {
            backupQueue = new RedisBackupQueue(this);// 备份队列

            String backUpQueueName = this.getBackUpQueueName();
            DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            logger.info("Backup queue[" + backUpQueueName + "]Monitoring begins：" + format.format(new Date()));
            task = backupQueue.popTask();
            while (task != null &&
                    !backUpQueueName.equals(task.getQueue()) &&
                    !RedisBackupQueue.MARKER.equals(task.getType())) {

                /**
                 * 判断任务状态，分别处理
                 * 1. 任务执行超时，且重试次数大于等于retry指定次数，则持久化到数据库
                 * 2. 任务执行超时，且重试次数小于retry指定次数，则重新放入任务队列
                 * 最后，如果满足以上条件，同时删除备份队列中的该任务
                 */
                TaskQueue taskQueue = new RedisTaskQueue(this, task.getQueue(), KMQueueManager.SAFE);
                // 获取任务状态
                Task.TaskStatus status = task.getTaskStatus();

                long currentTimeMillis = System.currentTimeMillis();// 当前时间戳
                long taskGenTimeMillis = status.getGenTimestamp();// 任务生成的时间戳
                long intervalTimeMillis = currentTimeMillis - taskGenTimeMillis;// 任务的存活时间
                if (intervalTimeMillis > this.aliveTimeout) {
                    if (pipeline != null) {
                        pipeline.process(taskQueue, task);// 彻底失败任务的处理
                    }
                    // 删除备份队列中的该任务
                    backupQueue.finishTask(task);
                }

                long taskExcTimeMillis = status.getExcTimestamp();// 任务执行的时间戳
                intervalTimeMillis = currentTimeMillis - taskExcTimeMillis;// 任务此次执行时间

                if (intervalTimeMillis > this.protectedTimeout) {// 任务执行超时

                    // 增加心跳健康检测
                    if (aliveDetectHandler != null) {

                        boolean isAlive = aliveDetectHandler.check(this, task);
                        if (isAlive) {// 当前任务还在执行
                            // 继续从备份队列中取出任务，进入下一次循环
                            task = backupQueue.popTask();
                            continue;
                        }
                    }

                    Task originTask = JSON.parseObject(JSON.toJSONString(task), Task.class);// 保留原任务数据，用于删除该任务

                    if (status.getRetry() < this.getRetryTimes()) {
                        // 重新放入任务队列
                        // 更新状态标记为retry
                        status.setState(Constant.RETRY);
                        // 更新重试次数retry + 1
                        status.setRetry(status.getRetry() + 1);
                        task.setTaskStatus(status);
                        // 放入任务队列的队首，优先处理
                        taskQueue.pushTaskToHeader(task);
                    } else {
                        if (pipeline != null) {
                            pipeline.process(taskQueue, task);// 彻底失败任务的处理
                        }
                    }

                    // 删除备份队列中的该任务
                    backupQueue.finishTask(originTask);
                }
                // 继续从备份队列中取出任务，进入下一次循环
                task = backupQueue.popTask();
            }

        } catch (Throwable e) {
            logger.info(e.getMessage());
            e.printStackTrace();
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
         * 备份队列名称
         */
        private String backUpQueueName;

        /**
         * redis连接池
         */
        private Pool<Jedis> pool;

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
         * redis获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted)，如果超时就抛异常，小于零则阻塞不确定的时间，默认-1
         */
        private Long poolMaxWaitMillis;

        /**
         * 任务超时重试次数，默认3次
         */
        private int retryTimes;

        /**
         * 失败任务（重试三次失败）的处理方式
         */
        private Pipeline pipeline;

        /**
         * 任务的存活超时时间。注意，该时间是任务从创建({@code new Task(...)})到销毁的总时间。单位：ms
         * <p>
         * 该值只针对安全队列起作用
         * <p>
         * 不设置默认为 Long.MAX_VALUE
         */
        private long aliveTimeout;

        /**
         * 任务执行的超时时间（一次执行），单位：ms
         * <p>
         * 该值只针对安全队列起作用
         * <p>
         * 不设置默认为 Long.MAX_VALUE
         */
        private long protectedTimeout;

        /**
         * 健康检查
         */
        private AliveDetectHandler aliveDetectHandler;

        /**
         * 创建Builder对象
         * <p>
         * 使用指定的redis连接池
         *
         * @param pool            redis连接池
         * @param backUpQueueName 备份队列名称
         */
        public Builder(Pool<Jedis> pool, String backUpQueueName) {
            Assert.notNull(pool, "Param pool can't null");
            Assert.notNull(backUpQueueName, "Param backUpQueueName can't null");

            this.retryTimes = 3;
            this.aliveTimeout = Long.MAX_VALUE;
            this.protectedTimeout = Long.MAX_VALUE;
            this.pool = pool;
            this.backUpQueueName = backUpQueueName;
            this.REDIS_CONN_MODE = this.REDIS_CONN_DEFAULT;
        }

        /**
         * 创建Builder对象
         *
         * @param host            redis host
         * @param port            redis port
         * @param backUpQueueName 备份队列名称
         */
        public Builder(String host, int port, String backUpQueueName) {
            Assert.notNull(host, "Param host can't null");
            Assert.notNull(port, "Param port can't null");
            Assert.notNull(backUpQueueName, "Param backUpQueueName can't null");

            this.retryTimes = 3;
            this.aliveTimeout = Long.MAX_VALUE;
            this.protectedTimeout = Long.MAX_VALUE;
            this.host = host;
            this.port = port;
            this.backUpQueueName = backUpQueueName;
            this.REDIS_CONN_MODE = this.REDIS_CONN_SINGLE;
        }

        /**
         * 创建Builder对象
         * <p>
         * 采用主从复制的方式创建redis连接池
         *
         * @param hostPort        逗号分隔的 host:port 列表
         * @param isSentinel      是否是主从复制
         * @param backUpQueueName 备份队列名称
         */
        public Builder(String hostPort, boolean isSentinel, String backUpQueueName) {
            Assert.isTrue(isSentinel, "Param isSentinel invalid");
            Assert.notNull(hostPort, "Param hostPort can't null");
            Assert.notNull(backUpQueueName, "Param backUpQueueName can't null");

            this.retryTimes = 3;
            this.aliveTimeout = Long.MAX_VALUE;
            this.protectedTimeout = Long.MAX_VALUE;
            this.sentinels = new HashSet<>();
            sentinels.addAll(Arrays.asList(hostPort.split(",")));
            this.backUpQueueName = backUpQueueName;
            this.REDIS_CONN_MODE = this.REDIS_CONN_SENTINEL;
        }

        /**
         * 设置redis连接池最大分配的连接数
         * <p>
         * 对使用{@link #Builder(Pool, String)}构造的Builder不起作用
         *
         * @param poolMaxTotal 连接池最大分配的连接数
         * @return 返回Builder
         */
        public Builder setMaxTotal(Integer poolMaxTotal) {
            if (poolMaxTotal <= 0) {
                throw new IllegalArgumentException("Param poolMaxTotal invalid");
            }
            this.poolMaxTotal = poolMaxTotal;
            return this;
        }

        /**
         * 设置redis连接池的最大空闲连接数
         * <p>
         * 对使用{@link #Builder(Pool, String)}构造的Builder不起作用
         *
         * @param poolMaxIdle 连接池的最大空闲连接数
         * @return 返回Builder
         */
        public Builder setMaxIdle(Integer poolMaxIdle) {
            if (poolMaxIdle < 0) {
                throw new IllegalArgumentException("Param poolMaxIdle invalid");
            }
            this.poolMaxIdle = poolMaxIdle;
            return this;
        }

        /**
         * 设置redis获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted)，如果超时就抛异常，小于零:阻塞不确定的时间，默认-1
         * <p>
         * 对使用{@link #Builder(Pool, String)}构造的Builder不起作用
         *
         * @param poolMaxWaitMillis 获取连接时的最大等待毫秒数
         * @return 返回Builder
         */
        public Builder setMaxWaitMillis(Long poolMaxWaitMillis) {
            this.poolMaxWaitMillis = poolMaxWaitMillis;
            return this;
        }

        /**
         * 设置失败任务（重试三次失败）的处理方式
         *
         * @param pipeline 失败任务（重试三次失败）的处理方式
         * @return 返回Builder
         */
        public Builder setPipeline(Pipeline pipeline) {
            this.pipeline = pipeline;
            return this;
        }

        /**
         * 任务超时重试次数，默认3次
         *
         * @param retryTimes 任务超时重试次数
         * @return 返回Builder
         */
        public Builder setRetryTimes(int retryTimes) {
            Assert.greaterThanEquals(retryTimes, 0, "Param retryTimes is negative");
            this.retryTimes = retryTimes;
            return this;
        }

        /**
         * 设置任务的存活超时时间。单位：ms
         * <p>
         * 注意，该时间是任务从创建({@code new Task(...)})到销毁的总时间
         * <p>
         * 传0 则采用默认值： Long.MAX_VALUE
         * <p>
         * 该值只针对安全队列起作用
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

        /**
         * 任务执行的超时时间（一次执行）。单位：ms
         * <p>
         * 传0 则采用默认值： Long.MAX_VALUE
         * <p>
         * 该值只针对安全队列起作用
         * <p>
         * 不设置默认为 Long.MAX_VALUE
         *
         * @param protectedTimeout 任务执行的超时时间
         * @return 返回Builder
         */
        public Builder setProtectedTimeout(long protectedTimeout) {
            Assert.greaterThanEquals(protectedTimeout, 0, "Param protectedTimeout is negative");
            if (protectedTimeout == 0) {
                protectedTimeout = Long.MAX_VALUE;
            }
            this.protectedTimeout = protectedTimeout;
            return this;
        }

        /**
         * 注册健康检查
         *
         * @param aliveDetectHandler 健康检测实现
         * @return 返回Builder
         */
        public Builder registerAliveDetectHandler(AliveDetectHandler aliveDetectHandler) {
            this.aliveDetectHandler = aliveDetectHandler;
            return this;
        }

        public BackupQueueMonitor build() {

            BackupQueueMonitor queueMonitor = new BackupQueueMonitor();

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
            queueMonitor.pool = this.pool;
            queueMonitor.backUpQueueName = this.backUpQueueName;
            queueMonitor.retryTimes = this.retryTimes;
            queueMonitor.pipeline = this.pipeline;
            queueMonitor.aliveTimeout = this.aliveTimeout;
            queueMonitor.protectedTimeout = this.protectedTimeout;
            queueMonitor.aliveDetectHandler = this.aliveDetectHandler;
            return queueMonitor;
        }
    }
}

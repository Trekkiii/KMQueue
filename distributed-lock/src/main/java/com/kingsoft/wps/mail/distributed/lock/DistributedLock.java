package com.kingsoft.wps.mail.distributed.lock;

import redis.clients.jedis.Jedis;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Redis分布式锁
 *
 * @author liuchunlong
 */
public class DistributedLock {

    private static final Lock NO_LOCK = new Lock(new UUID(0l, 0l), 0l);//超时锁     uuid:00000000-0000-0000-0000-000000000000     expiryTime:0

    private static final int ONE_SECOND = 1000;//1秒
    private static final int default_acquire_timeout_millis = Integer.getInteger("distribute.lock.default.acquire.timeout.millis", 10 * ONE_SECOND);//默认锁的请求超时时长
    private static final int default_expiry_millis = Integer.getInteger("distribute.lock.default.expiry.millis", 60 * ONE_SECOND);//默认锁的过期时长
    private static final int default_acquire_resolution_millis = Integer.getInteger("distribute.lock.default.acquire.resolution.millis", 100);//循环请求分布式锁线程休眠时长

    private Lock lock = null;//当前持有得锁

    private final Jedis jedis;
    private final String lockKey;//锁在Redis中的Key标记 (ex. distribute::lock, ...)
    private final int lockExpiryInMillis;//锁的过期时长
    private final int acquireTimeoutInMillis;//锁的请求超时时长
    private final UUID lockUUID;//锁的唯一标识

    public DistributedLock(String lockKey) {
        this(null, lockKey, default_acquire_timeout_millis, default_expiry_millis);
    }

    /**
     * 构造方法:<br>
     * - 使用默认锁的请求超时时长;<br>
     * - 使用默认锁的过期时长;<br>
     * - 锁的唯一标识UUID采用随机生成策略;<br>
     *
     * @param jedis   Jedis对象
     * @param lockKey 锁在Redis中的Key标记 (ex. distribute::lock, ...)
     */
    public DistributedLock(Jedis jedis, String lockKey) {
        this(jedis, lockKey, default_acquire_timeout_millis, default_expiry_millis);
    }

    /**
     * 构造方法:<br>
     * - 使用默认锁的过期时长;<br>
     * - 锁的唯一标识UUID采用随机生成策略;<br>
     *
     * @param jedis                  Jedis对象
     * @param lockKey                锁在Redis中的Key标记 (ex. distribute::lock, ...)
     * @param acquireTimeoutInMillis 请求超时时长(单位:毫秒)
     */
    public DistributedLock(Jedis jedis, String lockKey, int acquireTimeoutInMillis) {
        this(jedis, lockKey, acquireTimeoutInMillis, default_expiry_millis);
    }

    /**
     * 构造方法:<br>
     * - 锁的唯一标识UUID采用随机生成策略
     *
     * @param jedis                  Jedis对象
     * @param lockKey                锁在Redis中的Key标记 (ex. distribute::lock, ...)
     * @param acquireTimeoutInMillis 请求超时时长(单位:毫秒)
     * @param lockExpiryInMillis     锁的过期时长(单位:毫秒)
     */
    public DistributedLock(Jedis jedis, String lockKey, int acquireTimeoutInMillis, int lockExpiryInMillis) {
        this(jedis, lockKey, acquireTimeoutInMillis, lockExpiryInMillis, UUID.randomUUID());
    }

    /**
     * 构造方法
     *
     * @param jedis                  Jedis对象
     * @param lockKey                锁在Redis中的Key标记 (ex. distribute::lock, ...)
     * @param acquireTimeoutInMillis 请求超时时长(单位:毫秒)
     * @param lockExpiryInMillis     锁的过期时长(单位:毫秒)
     * @param uuid                   锁的唯一标识
     */
    public DistributedLock(Jedis jedis, String lockKey, int acquireTimeoutInMillis, int lockExpiryInMillis, UUID uuid) {
        this.jedis = jedis;
        this.lockKey = lockKey;
        this.acquireTimeoutInMillis = acquireTimeoutInMillis;
        this.lockExpiryInMillis = lockExpiryInMillis;
        this.lockUUID = uuid;
    }

    /**
     * 获取锁的唯一标识UUID
     *
     * @return lock uuid
     */
    public UUID getLockUUID() {
        return this.lockUUID;
    }

    /**
     * 获取锁在Redis中的Key标记
     *
     * @return lock key
     */
    public String getLockKey() {
        return this.lockKey;
    }

    /**
     * 锁的过期时长
     *
     * @return
     */
    public int getLockExpiryInMillis() {
        return lockExpiryInMillis;
    }

    /**
     * 锁的请求超时时长
     *
     * @return
     */
    public int getAcquireTimeoutInMillis() {
        return acquireTimeoutInMillis;
    }

    /**
     * 请求分布式锁
     *
     * @return 请求到锁返回true, 超时返回false
     * @throws InterruptedException 线程中断异常
     */
    public synchronized boolean acquire() throws InterruptedException {
        return acquire(jedis);
    }

    /**
     * 请求分布式锁
     *
     * @param jedis Jedis对象
     * @return 请求到锁返回true, 超时返回false
     * @throws InterruptedException 线程中断异常
     */
    public synchronized boolean acquire(Jedis jedis) throws InterruptedException {

        //采用"自旋获取锁"的方式,每次循环线程休眠100毫秒,直至请求锁超时
        int timeout = acquireTimeoutInMillis;//锁的请求超时时长

        while (timeout >= 0) {
            //创建一个新锁
            final Lock tLock = new Lock(lockUUID, System.currentTimeMillis() + lockExpiryInMillis);
            /**
             * 将当前锁(tLock)写入Redis中
             *      如果成功写入，Redis中不存在锁，获取锁成功;
             *      否则，Redis中已存在锁，获取锁失败;
             */
            if (jedis.setnx(lockKey, tLock.toString()) == 1) {
                this.lock = tLock;
                return true;
            }

            /**
             * 至此，Redis中已存在锁，获取锁失败，则需要进行如下操作:
             *      判断Redis中已存在的锁是否过期，如果过期则直接获取锁;
             *      否则，通过自旋获取锁
             */
            final String currentLockValue = jedis.get(lockKey);//获取Redis中已存在的锁的值
            final Lock currentLock = Lock.fromString(currentLockValue);//Redis中已存在的锁

            //如果Redis中已存在的锁(原始锁)已超时或者是当前线程的，则重新获取锁
            if (currentLock.isExpiredOrMine(lockUUID)) {
                String originLockValue = jedis.getSet(lockKey, tLock.toString());
                /**
                 * 这里还有个前置条件:
                 *      会对原始锁进行校验，jedis.get()和jedis.getSet()获取的锁必须是同一锁，重新获取锁才成功
                 */
                //特别的，当jedis.getSet()获取原始锁originLockValue为空时，应直接获取锁成功
                if (originLockValue == null) {
                    this.lock = tLock;
                    return true;
                }
                if (originLockValue != null && originLockValue.equals(currentLockValue)) {
                    this.lock = tLock;
                    return true;
                }
            }

            timeout -= default_acquire_resolution_millis;
            TimeUnit.MILLISECONDS.sleep(default_acquire_resolution_millis);
        }
        return false;
    }

    /**
     * 重新获取锁
     *
     * @return 如果获得锁返回true，否则返回false
     * @throws InterruptedException 线程中断异常
     */
    public boolean renew() throws InterruptedException {
        final Lock lock = Lock.fromString(jedis.get(lockKey));//获取Redis中已存在的锁
        if (!lock.isExpiredOrMine(lockUUID)) {//如果Redis中已存在的锁(原始锁)已超时或者是当前线程的，则重新获取锁
            return false;
        }
        return acquire(jedis);
    }

    /**
     * 释放锁
     */
    public synchronized void release() {
        release(jedis);
    }

    public synchronized void release(Jedis jedis) {
        if (isLocked()) {
            //存在一种情况，当前线程阻塞很长时间后再次执行，此时该线程持有的锁已经超时，并且其它线程获取了锁。这时当前线程就不应该再删除该锁
            if (this.lock.isExpired()) {//当前线程持有的锁已经超时
                final Lock lock = Lock.fromString(jedis.get(lockKey));//获取Redis中已存在的锁
                final UUID uuid = lock.getUUID();
                if (!this.lock.isMine(uuid)) {//如果Redis中已存在的锁(原始锁)不是当前线程的，则直接返回，不再释放锁
                    return;
                }
            }
            jedis.del(lockKey);
            this.lock = null;
        }
    }

    /**
     * 判断当前是否获取锁
     *
     * @return 返回布尔类型的值
     */
    public synchronized boolean isLocked() {
        return this.lock != null;
    }

    public synchronized long getLockExpiryTimeInMillis() {
        return this.lock.getExpiryTime();
    }

    /**
     * 锁
     */
    protected static class Lock {

        private UUID uuid;//锁的唯一标识uuid
        private long expiryTime;//锁的过期时间，注意，不是过期时长

        protected Lock(UUID uuid, long expiryTimeInMillis) {
            this.uuid = uuid;
            this.expiryTime = expiryTimeInMillis;
        }

        /**
         * 解析字符串,根据解析出的uuid和过期时间构造Lock
         *
         * @param text 字符串参数,参数格式:"*:*"
         * @return Lock    字符串转化的锁对象
         */
        protected static Lock fromString(String text) {
            try {
                String[] parts = text.split(":");
                UUID theUUID = UUID.fromString(parts[0]);
                long theTime = Long.parseLong(parts[1]);
                return new Lock(theUUID, theTime);
            } catch (Exception e) {
                return NO_LOCK;
            }
        }

        public UUID getUUID() {
            return uuid;
        }

        public long getExpiryTime() {
            return expiryTime;
        }

        @Override
        public String toString() {
            return uuid.toString() + ":" + expiryTime;
        }

        /**
         * 判断锁是否超时，如果锁的过期时间小于当前系统时间，则判定锁超时
         *
         * @return 返回布尔类型的值
         */
        boolean isExpired() {
            return getExpiryTime() < System.currentTimeMillis();
        }

        /**
         * 判断锁是否是当前线程拥有的锁
         *
         * @param otherUUID
         * @return
         */
        boolean isMine(UUID otherUUID) {
            return this.getUUID().equals(otherUUID);
        }

        /**
         * 判断锁是否超时或者锁是当前线程拥有的锁
         *
         * @param otherUUID 锁的唯一标识uuid
         * @return 返回布尔类型的值
         */
        boolean isExpiredOrMine(UUID otherUUID) {
            return this.isExpired() || this.getUUID().equals(otherUUID);
        }
    }
}

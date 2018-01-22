package com.kingsoft.wps.mail;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.util.Pool;

import java.util.List;

/**
 * Created by 刘春龙 on 2018/1/19.
 */
public class JedisTest {

    private Pool<Jedis> pool;

    @Before
    public void init() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        this.pool = new JedisPool(jedisPoolConfig, "127.0.0.1", 6379);
    }

    @Test
    public void brpopTest() {
//        List<String> rs = this.pool.getResource().brpop(0, "testList");
//        System.out.println(rs);// [testList, liucl]
    }

    @Test
    public void sremTest() {
        Long rs = this.pool.getResource().srem("worker1_queue_unique", "a509bd99-1071-4de1-9220-a280b0a4f47a");
        System.out.println(rs);
    }
}

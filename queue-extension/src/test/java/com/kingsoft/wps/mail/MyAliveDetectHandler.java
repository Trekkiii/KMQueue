package com.kingsoft.wps.mail;

import com.kingsoft.wps.mail.queue.Task;
import com.kingsoft.wps.mail.queue.extension.monitor.AliveDetectHandler;
import com.kingsoft.wps.mail.queue.extension.monitor.BackupQueueMonitor;
import redis.clients.jedis.Jedis;

/**
 * Created by 刘春龙 on 2018/1/23.
 */
public class MyAliveDetectHandler implements AliveDetectHandler {

    @Override
    public boolean check(BackupQueueMonitor monitor, Task task) {
        Jedis jedis = monitor.getResource();
        String value = jedis.get(task.getId() + ALIVE_KEY_SUFFIX);
        return value != null && !"".equals(value.trim());
    }
}

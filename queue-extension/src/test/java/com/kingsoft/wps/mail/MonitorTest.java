package com.kingsoft.wps.mail;

import com.kingsoft.wps.mail.queue.config.Constant;
import com.kingsoft.wps.mail.queue.extension.monitor.BackupQueueMonitor;
import com.kingsoft.wps.mail.utils.KMQUtils;
import org.junit.Test;

/**
 * Created by 刘春龙 on 2018/1/22.
 */
public class MonitorTest {

    @Test
    public void monitorTaskTest() {

        // 健康检测
        MyAliveDetectHandler detectHandler = new MyAliveDetectHandler();
        // 任务彻底失败后的处理，需要实现Pipeline接口，自行实现处理逻辑
        MyPipeline pipeline = new MyPipeline();
        // 根据任务队列的名称构造备份队列的名称，注意：这里的任务队列参数一定要和KMQueueManager构造时传入的一一对应。
        String backUpQueueName = KMQUtils.genBackUpQueueName("worker1_queue", "worker2_queue:safe");
        // 构造Monitor监听器
        BackupQueueMonitor backupQueueMonitor = new BackupQueueMonitor.Builder("127.0.0.1", 6379, backUpQueueName)
                .setMaxWaitMillis(-1L)
                .setMaxTotal(600)
                .setMaxIdle(300)
                .setAliveTimeout(Constant.ALIVE_TIMEOUT)
                .setProtectedTimeout(Constant.PROTECTED_TIMEOUT)
                .setRetryTimes(Constant.RETRY_TIMES)
                .registerAliveDetectHandler(detectHandler)
                .setPipeline(pipeline).build();
        // 执行监听
        backupQueueMonitor.monitor();
    }
}

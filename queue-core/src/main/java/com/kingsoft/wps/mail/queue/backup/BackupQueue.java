package com.kingsoft.wps.mail.queue.backup;

import com.kingsoft.wps.mail.queue.Task;

/**
 * Created by 刘春龙 on 2017/3/5.
 * <p>
 * 安全队列对应的备份队列
 */
public abstract class BackupQueue {

    /**
     * 初始化备份队列，添加备份队列循环标记
     */
    public abstract void initQueue();

    /**
     * 获取队列名
     *
     * @return 队列名
     */
    public abstract String getName();

    /**
     * 从队尾取一个任务，然后再将其放入队首
     *
     * @return 任务
     */
    public abstract Task popTask();

    /**
     * 备份队列的任务完成，删除备份队列中的该任务
     *
     * @param task 超时任务
     */
    public abstract void finishTask(Task task);
}

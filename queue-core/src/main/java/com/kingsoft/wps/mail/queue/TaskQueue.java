package com.kingsoft.wps.mail.queue;

/**
 * Created by 刘春龙 on 2017/3/3.
 */
public abstract class TaskQueue {

    /**
     * 获取队列名
     *
     * @return 队列名
     */
    public abstract String getName();

    /**
     * 获取队列的模式：安全队列 or 默认的普通队列
     *
     * @return 队列模式
     */
    public abstract String getMode();

    /**
     * 往队列中添加任务
     *
     * @param task 队列任务
     */
    public abstract Task pushTask(Task task);

    /**
     * 往队首添加任务
     *
     * @param task 队列任务
     */
    public abstract void pushTaskToHeader(Task task);

    /**
     * 从任务队列里取任务
     * <p>
     * 任务状态state不变，默认值为`normal`
     *
     * @return 队列任务
     */
    public abstract Task popTask();

    /**
     * 队列任务完成
     *
     * @param task 队列任务
     */
    protected abstract void finishTask(Task task);
}

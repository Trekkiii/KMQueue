package com.kingsoft.wps.mail.queue.extension.monitor;

import com.kingsoft.wps.mail.queue.Task;
import com.kingsoft.wps.mail.queue.TaskQueue;

/**
 * Created by 刘春龙 on 2018/1/19.
 *
 * 失败任务（重试三次失败）的处理
 */
public interface Pipeline {

    /**
     * 失败任务的处理
     *
     * @param taskQueue 任务所属队列
     * @param task 任务
     */
    public void process(TaskQueue taskQueue, Task task);
}

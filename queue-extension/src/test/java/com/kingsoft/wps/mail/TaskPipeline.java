package com.kingsoft.wps.mail;

import com.alibaba.fastjson.JSON;
import com.kingsoft.wps.mail.queue.Task;
import com.kingsoft.wps.mail.queue.TaskQueue;
import com.kingsoft.wps.mail.queue.extension.monitor.Pipeline;

/**
 * Created by 刘春龙 on 2018/1/22.
 */
public class TaskPipeline implements Pipeline {
    @Override
    public void process(TaskQueue taskQueue, Task task) {
        System.out.println("Task is timeout，task - " + JSON.toJSON(task));
    }
}

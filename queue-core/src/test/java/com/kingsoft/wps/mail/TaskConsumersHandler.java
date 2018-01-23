package com.kingsoft.wps.mail;

import com.kingsoft.wps.mail.queue.TaskHandler;

/**
 * Created by 刘春龙 on 2018/1/19.
 */
public class TaskConsumersHandler implements TaskHandler {
    @Override
    public void handle(String data, Object... params) {
        System.out.println("获取任务数据：" + data);
    }
}

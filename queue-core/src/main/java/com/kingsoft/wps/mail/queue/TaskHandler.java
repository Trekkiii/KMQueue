package com.kingsoft.wps.mail.queue;

/**
 * Created by 刘春龙 on 2017/3/6.
 */
public interface TaskHandler {

    /**
     * 业务处理
     * @param data task任务数据
     * @param params 业务自定义参数
     */
    void handle(String data, Object... params);
}

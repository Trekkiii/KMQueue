package com.kingsoft.wps.mail.queue;

/**
 * Created by 刘春龙 on 2017/3/6.
 */
public interface TaskHandler {

    void handle(String data);
}

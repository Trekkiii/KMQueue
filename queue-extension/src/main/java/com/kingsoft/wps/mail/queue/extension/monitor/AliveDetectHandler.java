package com.kingsoft.wps.mail.queue.extension.monitor;

import com.kingsoft.wps.mail.queue.Task;

/**
 * Created by 刘春龙 on 2018/1/23.
 * <p>
 * 健康检查
 * <p>
 * 检查正在执行的任务是否还在执行（存活），
 * 为了防止耗时比较久的任务（任务的执行时间超出了通过队列管理器配置的任务执行超时时间 - 默认值：{@link com.kingsoft.wps.mail.queue.config.Constant#PROTECTED_TIMEOUT}）
 * 会被备份队列监听器检测到并重新放入任务队列执行（因为备份队列监听器会把超出通过队列管理器配置的任务执行超时时间的任务当作是失败的任务（参考 https://github.com/fnpac/KMQueue#什么是失败任务？）并进行重试）。
 * <p>
 * 通过这种检测机制，可以保证{@link #check(Task)}返回为true的任务（任务还在执行）不会被备份队列监听器重新放入任务队列重试。
 * <p>
 * 这里只是提供一个接口，用户需要自己实现执行任务的健康检测。
 * 一个比较简单的实现方式就是起一个定时job，每隔n毫秒检查线程中正在执行任务的状态，在redis中以 "任务的id + {@link AliveDetectHandler#ALIVE_KEY_SUFFIX}" 为key，ttl 为 n+m 毫秒（m < n, m用于保证两次job的空窗期），标记正在执行的任务。
 * 然后{@link AliveDetectHandler}的实现类根据task去检查redis中是否存在该key，如果存在，返回true
 */
public interface AliveDetectHandler {

    public static final String ALIVE_KEY_SUFFIX = "_alive";

    /**
     * 健康检查
     * <p>
     * 任务正在执行返回true，否则（任务挂了、任务执行完成）返回false
     *
     * @param monitor 份队列监听器
     * @param task 要检查的任务
     * @return 检查结果，任务正在执行返回true，否则（任务挂了、任务执行完成）返回false
     */
    boolean check(BackupQueueMonitor monitor, Task task);
}

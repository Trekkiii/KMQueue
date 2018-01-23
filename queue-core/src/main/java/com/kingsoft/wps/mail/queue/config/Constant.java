package com.kingsoft.wps.mail.queue.config;

/**
 * Created by 刘春龙 on 2017/3/5.
 */
public class Constant {

    //    private static final String DISTR_LOCK_SUFFIX = "_lock";
    // 用于队列任务唯一性标记，redis set key
    public static final String UNIQUE_SUFFIX = "_unique";

    /**
     * 标记任务为正常执行状态
     */
    public static final String NORMAL = "normal";

    /**
     * 标记任务为重试执行状态
     */
    public static final String RETRY = "retry";

    /**
     * 任务的存活时间。单位：ms
     * <p>
     * 注意，该时间是任务从创建({@code new Task(...)})到销毁的总时间
     * <p>
     * 该值只针对安全队列起作用
     */
    @Deprecated
    public static final long ALIVE_TIMEOUT = 10 * 60 * 1000;

    /**
     * 任务执行的超时时间（一次执行）。单位：ms
     * <p>
     * 该值只针对安全队列起作用
     * <p>
     * TODO 后续会加入心跳健康检测
     */
    @Deprecated
    public static final long PROTECTED_TIMEOUT = 3 * 60 * 1000;

    /**
     * 任务重试次数
     */
    @Deprecated
    public static final int RETRY_TIMES = 3;
}

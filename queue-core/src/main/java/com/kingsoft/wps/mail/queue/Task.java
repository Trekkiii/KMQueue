package com.kingsoft.wps.mail.queue;

import com.kingsoft.wps.mail.queue.config.Constant;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by 刘春龙 on 2018/1/18.
 */
public class Task implements Serializable {

    /**
     * 任务队列名称
     */
    private String queue;

    /**
     * 任务唯一标识，UUID
     */
    private String id;

    /**
     * 任务类型
     */
    private String type;

    /**
     * 任务数据
     */
    private String data;

    /**
     * 队列中任务是否可以存在多个相同任务，以id作为同一任务的标识
     */
    private boolean isUnique;

    /**
     * 任务状态
     */
    private TaskStatus status;

    private Task() {
    }

    /**
     * 构造任务实体
     *
     * @param queue  任务队列
     * @param uid    任务的唯一标识，传null，则默认使用uuid生成策略
     *               如果业务需要区分队列任务的唯一性，请自行生成uid参数，
     *               否则队列默认使用uuid生成策略，这会导致即使data数据完全相同的任务也会被当作两个不同的任务处理。
     * @param type   任务类型，用于业务逻辑的处理，你可以根据不同的type任务类型，调用不同的handler去处理，可以不传。
     * @param data   任务数据
     * @param status 任务状态
     */
    public Task(String queue, String uid, String type, String data, TaskStatus status) {
        this(queue, uid, false, type, data, status);
    }

    /**
     * 构造任务实体
     *
     * @param queue    任务队列
     * @param uid      任务的唯一标识，传null，则默认使用uuid生成策略
     *                 如果业务需要区分队列任务的唯一性，请自行生成uid参数，
     *                 否则队列默认使用uuid生成策略，这会导致即使data数据完全相同的任务也会被当作两个不同的任务处理。
     * @param isUnique 是否是唯一任务，即队列中同一时刻只存在一个该任务。
     * @param type     任务类型，用于业务逻辑的处理，你可以根据不同的type任务类型，调用不同的handler去处理，可以不传。
     * @param data     任务数据
     * @param status   任务状态
     */
    public Task(String queue, String uid, boolean isUnique, String type, String data, TaskStatus status) {
        this.queue = queue;
        if (uid == null || "".equals(uid)) {
            uid = UUID.randomUUID().toString();
        }
        this.id = uid;
        this.type = type;
        this.isUnique = isUnique;
        this.data = data;
        this.status = status;
    }

    /**
     * 队列中任务是否可以存在多个相同任务，以id作为同一任务的标识
     *
     * @return 是否是唯一性任务
     */
    public boolean isUnique() {
        return isUnique;
    }

    /**
     * 队列中任务是否可以存在多个相同任务，以id作为同一任务的标识
     *
     * @param isUnique 是否是唯一性任务
     */
    public void setUnique(boolean isUnique) {
        this.isUnique = isUnique;
    }

    /**
     * 获取任务队列名称
     *
     * @return 任务队列名称
     */
    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public TaskStatus getTaskStatus() {
        return status;
    }

    public void setTaskStatus(TaskStatus status) {
        this.status = status;
    }

    public static class TaskStatus {
        /**
         * 任务状态state，normal or retry
         */
        private String state;

        /**
         * 任务生成的时间戳，每次重试不会重置
         */
        private long genTimestamp;

        /**
         * 任务执行的时间戳，每次重试时，都会在该任务从任务队列中取出后（开始执行前）重新设置为当前时间
         */
        private long excTimestamp;

        /**
         * 任务超时后重试的次数
         */
        private int retry;

        public TaskStatus() {
            this.state = Constant.NORMAL;
            this.genTimestamp = System.currentTimeMillis();
            this.excTimestamp = 0;
            this.retry = 0;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }

        /**
         * 获取任务生成的时间戳，每次重试不会重置
         *
         * @return 任务生成的时间戳
         */
        public long getGenTimestamp() {
            return genTimestamp;
        }

        public void setGenTimestamp(long genTimestamp) {
            this.genTimestamp = genTimestamp;
        }

        /**
         * 获取任务执行的时间戳，每次重试时，都会在该任务从任务队列中取出后（开始执行前）重新设置为当前时间
         *
         * @return 任务的执行的时间戳
         */
        public long getExcTimestamp() {
            return excTimestamp;
        }

        public void setExcTimestamp(long excTimestamp) {
            this.excTimestamp = excTimestamp;
        }

        public int getRetry() {
            return retry;
        }

        public void setRetry(int retry) {
            this.retry = retry;
        }
    }

    /**
     * 执行任务
     * <p>
     * 任务状态state不变
     *
     * @param clazz 任务处理器class
     */
    public void doTask(KMQueueManager kmQueueManager, Class clazz) {

        // 获取任务所属队列
        TaskQueue taskQueue = kmQueueManager.getTaskQueue(this.getQueue());
        String queueMode = taskQueue.getMode();
        if (KMQueueManager.SAFE.equals(queueMode)) {// 安全队列
            try {
                handleTask(clazz);
            } catch (Throwable e) {
                e.printStackTrace();
            }
            // 任务执行完成，删除备份队列的相应任务
            taskQueue.finishTask(this);
        } else {// 普通队列
            handleTask(clazz);
        }
    }

    /**
     * 执行任务
     *
     * @param clazz 任务执行器
     */
    private void handleTask(Class clazz) {
        try {
            TaskHandler handler = (TaskHandler) clazz.newInstance();
            handler.handle(this.data);
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}

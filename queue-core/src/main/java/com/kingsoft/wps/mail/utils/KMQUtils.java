package com.kingsoft.wps.mail.utils;

import com.kingsoft.wps.mail.queue.KMQueueAdapter;
import sun.misc.BASE64Encoder;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by 刘春龙 on 2018/1/22.
 */
public class KMQUtils {

    /**
     * 生成备份队列的名称
     *
     * @param queues 备份队列所对应的任务队列
     * @return 备份队列的名称
     */
    public static String genBackUpQueueName(String ...queues) {
        // 生成备份队列名称
        try {
            MessageDigest md5Digest = MessageDigest.getInstance("MD5");
            BASE64Encoder base64Encoder = new BASE64Encoder();

            // 获取队列名称
            StringBuilder queueNameMulti = new StringBuilder();
            // Stream 是支持并发操作的，为了避免竞争，对于reduce线程都会有独立的result，combiner的作用在于合并每个线程的result得到最终结果
            queueNameMulti = Arrays.stream(queues)
                    .map(s -> s.trim().split(":")[0])
                    .reduce(queueNameMulti,
                            StringBuilder::append,
                            StringBuilder::append);
            try {
                return KMQueueAdapter.BACK_UP_QUEUE_PREFIX + base64Encoder.encode(md5Digest.digest(queueNameMulti.toString().getBytes("UTF-8")));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return "";
    }

    public static String genBackUpQueueName(List<String> queues) {
        return genBackUpQueueName((String[]) queues.toArray());
    }
}

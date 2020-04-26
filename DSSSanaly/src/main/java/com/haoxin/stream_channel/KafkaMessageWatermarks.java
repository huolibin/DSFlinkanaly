package com.haoxin.stream_channel;

import com.haoxin.log.KafkaMessage;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/4/20 17:53
 */
public class KafkaMessageWatermarks implements AssignerWithPeriodicWatermarks<KafkaMessage> {

    private final long maxOutOfOrderness = 10000; // 最大乱序时间10秒
    private long currentTimestamp = 0;//当前最大时间
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(KafkaMessage kafkaMessage, long l) {
        long timestamp = kafkaMessage.getTimestamp();
        currentTimestamp = Math.max(timestamp, currentTimestamp);
        return timestamp;
    }
}

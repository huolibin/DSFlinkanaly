package com.haoxin.stream_brandlike.util;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/4/8 16:12
 * 创建一个kafkaevent类，是kafka接收的数据类型
 */
public class KafkaEvent {
    private final static String splitword ="##";
    private String word;
    private  int frequency;
    private  long timestamp;

    public KafkaEvent() {
    }

    public KafkaEvent(String word, int frequency, long timestamp) {
        this.word = word;
        this.frequency = frequency;
        this.timestamp = timestamp;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public static KafkaEvent fromString(String eventStr){
        String[] split = eventStr.split(splitword);
        return new KafkaEvent(split[0],Integer.valueOf(split[1]),Long.valueOf(split[2]));
    }

    @Override
    public String toString() {
        return word+splitword+frequency+splitword+timestamp;
    }
}

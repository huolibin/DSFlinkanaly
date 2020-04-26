package com.haoxin.log;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/4/20 16:00
 * 产品数据输入到kafka的数据格式
 */
public class KafkaMessage {
    private String jsonmessage; //json格式的消息
    private int count;//消息的次数
    private Long timestamp; //消息的生成时间

    public String getJsonmessage() {
        return jsonmessage;
    }

    public void setJsonmessage(String jsonmessage) {
        this.jsonmessage = jsonmessage;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "KafkaMessage{" +
                "jsonmessage='" + jsonmessage + '\'' +
                ", count=" + count +
                ", timestamp=" + timestamp +
                '}';
    }
}

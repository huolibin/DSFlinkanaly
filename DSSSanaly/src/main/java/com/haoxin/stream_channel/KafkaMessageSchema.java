package com.haoxin.stream_channel;

import com.alibaba.fastjson.JSON;
import com.haoxin.log.KafkaMessage;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/4/20 16:09
 *
 * d对kafkamessage 的序列化
 */
public class KafkaMessageSchema implements DeserializationSchema<KafkaMessage>, SerializationSchema<KafkaMessage> {

    private static final long serialVersionUID = 2323370181669758L;

    @Override
    public KafkaMessage deserialize(byte[] bytes) throws IOException {
        String jsonString = new String(bytes);
        KafkaMessage kafkaMessage = JSON.parseObject(jsonString, KafkaMessage.class);
        return kafkaMessage;
    }

    @Override
    public boolean isEndOfStream(KafkaMessage kafkaMessage) {
        return false;
    }

    @Override
    public byte[] serialize(KafkaMessage kafkaMessage) {
        String jsonstring = JSON.toJSONString(kafkaMessage);
        return jsonstring.getBytes();
    }

    @Override
    public TypeInformation<KafkaMessage> getProducedType() {
        return TypeInformation.of(KafkaMessage.class);
    }
}

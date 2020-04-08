package com.haoxin.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/4/8 16:19
 */
public class KafkaEventSchema implements DeserializationSchema<KafkaEvent>, SerializationSchema<KafkaEvent> {

    private static final long serialVersionUID = 6154188370181669758L;

    @Override
    public KafkaEvent deserialize(byte[] message) throws IOException {
        return KafkaEvent.fromString(new String(message));
    }

    @Override
    public boolean isEndOfStream(KafkaEvent kafkaEvent) {
        return false;
    }

    @Override
    public byte[] serialize(KafkaEvent kafkaEvent) {
        return kafkaEvent.toString().getBytes();
    }

    @Override
    public TypeInformation<KafkaEvent> getProducedType() {
        return TypeInformation.of(KafkaEvent.class);
    }
}

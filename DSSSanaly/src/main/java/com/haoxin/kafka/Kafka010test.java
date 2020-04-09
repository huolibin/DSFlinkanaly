package com.haoxin.kafka;

import com.haoxin.util.KafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;


/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/4/8 15:36
 * flink对kafka数据的消费 计算和写入
 */
public class Kafka010test {
    public static void main(String[] args) {

        args = new String[]{"--input-topic",KafkaUtil.KAFKA_IN_TOPIC,"--output-topic",KafkaUtil.KAFKA_OUT_TOPIC,"--bootstrap.servers",KafkaUtil.KAFKA_SERVERS,
                "--zookeeper.connect",KafkaUtil.ZOOKEEPER_CONNECT,"--group.id",KafkaUtil.GROUP_ID};
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 5) {
            System.out.println("参数不全，请重新检查:"+"Usage: Kafka --input-topic <topic> --output-topic <topic> " +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> --group.id <some id>");
            return;
        }

        //flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4,10000));
        env.enableCheckpointing(5000);//每5秒创建一个checkpoint
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //接收source and 处理数据
        DataStream<KafkaEvent> input = env.addSource(new FlinkKafkaConsumer010<String>(parameterTool.get("input-topic"), new SimpleStringSchema(), parameterTool.getProperties())).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                if (s.split(",").length == 3){
                return true;}
                return false;
            }
        })//add filter
                .map(new MapFunction<String, KafkaEvent>() {
            @Override
            public KafkaEvent map(String s) throws Exception {
                String[] split = s.split(",");
                return new KafkaEvent(split[0],Integer.valueOf(split[1]),Long.valueOf(split[2]));
            }
        });

        DataStream<KafkaEvent> map = input.assignTimestampsAndWatermarks(new CustormWatermark())
                .keyBy("word")
                .map(new MyMapAdd());

        //输出数据
        map.addSink(new FlinkKafkaProducer010<KafkaEvent>(parameterTool.get("output-topic"),new KafkaEventSchema(),parameterTool.getProperties()));

        String jobName = Kafka010test.class.getSimpleName();
        try {
            env.execute(jobName);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static class CustormWatermark implements AssignerWithPeriodicWatermarks<KafkaEvent>{

        private static final long serialVersionUID = -2154168370181669758L;
        private final long maxOutOfOrderness = 10000; // 最大乱序时间10秒
        private long currentMaxTimestamp = 0L;  //当前最大时间
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp -maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(KafkaEvent kafkaEvent, long l) {
            Long timestamp = kafkaEvent.getTimestamp();//确定event_time
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }

    private static class MyMapAdd extends RichMapFunction<KafkaEvent,KafkaEvent>{
        private static final long serialVersionUID = -2344188370181669758L;
        private  transient ValueState<Integer> currentToCount;
        @Override
        public KafkaEvent map(KafkaEvent kafkaEvent) throws Exception {
            Integer value = currentToCount.value();
            if (value == null) {
                value=0;
            }
            value +=kafkaEvent.getFrequency();
            currentToCount.update(value);
            return new KafkaEvent(kafkaEvent.getWord(),value,kafkaEvent.getTimestamp());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            currentToCount = getRuntimeContext().getState(new ValueStateDescriptor<>("currentToCount", Integer.class));
        }
    }
}

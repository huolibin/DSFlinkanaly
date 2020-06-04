package com.haoxin.stream_chaomanchaonv.task;

import com.haoxin.stream_brandlike.util.KafkaEvent;
import com.haoxin.stream_brandlike.util.KafkaEventSchema;
import com.haoxin.stream_chaomanchaonv.map.ChaoManAndWomanByreduceMap;
import com.haoxin.stream_chaomanchaonv.map.ChaoManAndWomanMap;
import com.haoxin.stream_chaomanchaonv.reduce.ChaoManAndWomanReduce;
import com.haoxin.stream_chaomanchaonv.reduce.ChaoManAndWomanSink;
import com.haoxin.stream_chaomanchaonv.reduce.ChaoManAndWomanfinalReduce;
import com.haoxin.stream_chaomanchaonv.util.ChaoManAndWomanInfo;
import com.mongodb.lang.Nullable;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/3 16:39
 * 数据来源是kafka的浏览产品数据
 */
public class ChaoManAndWomanTask {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "E:\\hadoop2.6\\hadoop-2.6.3");
        args = new String[]{"--input-topic", "scanProductLog", "--bootstrap.servers", "192.168.71.13:9092",
                "--zookeeper.connect", "192.168.71.10:2181", "--group.id", "pindaoxinxiandu1", "--winsdows.size", "50"};

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 5) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-topic <topic>" +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> --group.id <some id>");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //source
        DataStream<com.haoxin.stream_brandlike.util.KafkaEvent> input = env
                .addSource(new FlinkKafkaConsumer010<com.haoxin.stream_brandlike.util.KafkaEvent>(parameterTool.getRequired("input-topic"),
                        new KafkaEventSchema(),
                        parameterTool.getProperties())
                        .assignTimestampsAndWatermarks(new CustomWatermarkExtractor()));

        //trans
        DataStream<ChaoManAndWomanInfo> chaoManAndWomanMap = input.flatMap(new ChaoManAndWomanMap());
        DataStream<ChaoManAndWomanInfo> chaoManAndWomanReduce = chaoManAndWomanMap.keyBy("groupbyfield").timeWindowAll(Time.minutes(1)).reduce(new ChaoManAndWomanReduce());
        DataStream<ChaoManAndWomanInfo> chaoManAndWomanflatMap = chaoManAndWomanReduce.flatMap(new ChaoManAndWomanByreduceMap());
        DataStream<ChaoManAndWomanInfo> finalresult = chaoManAndWomanflatMap.keyBy("groupbyfield").reduce(new ChaoManAndWomanfinalReduce());


        //sink
        finalresult.addSink(new ChaoManAndWomanSink());//chaotype 结果写入Mongo

        try {
            env.execute("ChaoManAndWoman analy");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class CustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<KafkaEvent> {

        private static final long serialVersionUID = -742759155861320823L;

        private long currentTimestamp = Long.MIN_VALUE;

        @Override
        public long extractTimestamp(KafkaEvent event, long previousElementTimestamp) {
            // the inputs are assumed to be of format (message,timestamp)
            this.currentTimestamp = event.getTimestamp();
            return event.getTimestamp();
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
        }
    }
}

package com.haoxin.stream_brandlike.task;

import com.haoxin.log.KafkaMessage;
import com.haoxin.stream_brandlike.map.BrandLikeMap;
import com.haoxin.stream_brandlike.reduce.BrandLikeReduce;
import com.haoxin.stream_brandlike.reduce.BrandLikesinkreduce;
import com.haoxin.stream_brandlike.util.BrandLike;
import com.haoxin.stream_brandlike.util.KafkaEvent;
import com.haoxin.stream_brandlike.util.KafkaEventSchema;
import com.haoxin.stream_channel.KafkaMessageSchema;
import com.haoxin.stream_channel.KafkaMessageWatermarks;
import com.haoxin.stream_xinxiandu.PindaoXinXianDu;
import com.haoxin.stream_xinxiandu.PindaoXinXianDuMap;
import com.haoxin.stream_xinxiandu.PindaoXinXianDuReduce;
import com.haoxin.stream_xinxiandu.PindaoXinXianDusinkreduce;
import com.mongodb.lang.Nullable;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/7 16:39
 *  新鲜度是指新用户跟老用户之比
 *  数据来源是kafka的产品数据
 *  计算频道的新鲜度 ，并把结果写入hbase
 */
public class BrandLikeProcessData {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","E:\\hadoop2.6\\hadoop-2.6.3");
        args = new String[]{"--input-topic","test1","--bootstrap.servers","192.168.71.13:9092",
                "--zookeeper.connect","192.168.71.10:2181","--group.id","pindaoxinxiandu1","--winsdows.size","50"};

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
        DataStream<KafkaEvent> input = env
                .addSource(new FlinkKafkaConsumer010<KafkaEvent>(parameterTool.getRequired("input-topic"),
                        new KafkaEventSchema(),
                        parameterTool.getProperties())
                        .assignTimestampsAndWatermarks(new CustomWatermarkExtractor()));

        //trans
        DataStream<BrandLike> brandLikeMap = input.flatMap(new BrandLikeMap());
        DataStream<BrandLike> brandLikeReduce = brandLikeMap.keyBy("groupfield").timeWindowAll(Time.minutes(1)).reduce(new BrandLikeReduce());

        //sink
        brandLikeReduce.addSink(new BrandLikesinkreduce());

        try {
            env.execute("brandlike analy");
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

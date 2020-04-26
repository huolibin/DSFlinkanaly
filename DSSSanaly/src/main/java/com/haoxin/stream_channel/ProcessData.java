package com.haoxin.stream_channel;

import com.alibaba.fastjson.JSON;
import com.haoxin.log.KafkaMessage;
import com.haoxin.log.Userscanlog;
import com.haoxin.util.KafkaUtil;
import com.haoxin.util.RedisUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/4/20 15:40
 * 频道分析之实时热点排行
 * flink处理产品pindao数据 输入到redis
 */
public class ProcessData {
    public static void main(String[] args) throws NullPointerException {
        args = new String[]{"--input-topic", "testmessage", "--bootstrap.servers", KafkaUtil.KAFKA_SERVERS,
                "--zookeeper.connect", KafkaUtil.ZOOKEEPER_CONNECT, "--group.id", "mymessage3","--winsdows.size","50","--winsdows.slide","5"};
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 6) {
            System.out.println("参数不全，请重新检查:" + "Usage: Kafka --input-topic <topic> --output-topic <topic> " +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> --group.id <some id>");
            return;
        }

        //flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(10000);//每10秒创建一个checkpoint
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer010<KafkaMessage> flinkKafkaConsumer1 = new FlinkKafkaConsumer010<KafkaMessage>("testmessage", new KafkaMessageSchema(), parameterTool.getProperties());
        DataStream<KafkaMessage> input = env.addSource(flinkKafkaConsumer1).assignTimestampsAndWatermarks(new KafkaMessageWatermarks());
        DataStream<PindaoHost> map = input.map(new PindaoKafkaMap());
        //countwindow
        DataStream<PindaoHost> reduce = map.keyBy("pindaoid").countWindow(50L,5L).reduce(new PindaoReduce());
//        //timewindow
//        DataStream<PindaoHost> reduce = map.keyBy("pindaoid").timeWindow(Time.minutes(1), Time.seconds(30)).reduce(new PindaoReduce());

        //redis sink
       reduce.addSink(new SinkFunction<PindaoHost>() {
           @Override
           public void invoke(PindaoHost value, Context context) throws Exception {
               long pindaoid = value.getPindaoid();
               long count = value.getCount();
               System.out.println("输出==pindaoid"+pindaoid+":"+count);
               RedisUtil.jedis.lpush("pindaoid:"+pindaoid,count+"");
           }
       }).name("pdrdreduce");


        try {
            env.execute("ProcessData");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static class PindaoKafkaMap extends RichMapFunction<KafkaMessage, PindaoHost> {

        @Override
        public PindaoHost map(KafkaMessage kafkaMessage) throws Exception {
            String jsonmessage = kafkaMessage.getJsonmessage();
            Userscanlog userscanlog = JSON.parseObject(jsonmessage, Userscanlog.class);
            PindaoHost pindaoHost = new PindaoHost();
            long pindaoids = userscanlog.getPindaoids();
            pindaoHost.setPindaoid(pindaoids);
            pindaoHost.setCount(Long.valueOf(kafkaMessage.getCount()));

            return pindaoHost;
        }
    }
}

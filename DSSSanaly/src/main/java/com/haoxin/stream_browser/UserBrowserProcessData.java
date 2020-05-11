package com.haoxin.stream_browser;

import com.haoxin.log.KafkaMessage;
import com.haoxin.stream_channel.KafkaMessageSchema;
import com.haoxin.stream_channel.KafkaMessageWatermarks;
import com.haoxin.stream_network.UserNetwork;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/11 16:39
 * 数据来源是kafka的产品数据
 * <p>
 * 计算网络的数据（各个网络的pv次数，新增个数，老用户数） ，并把结果写入hbase
 */
public class UserBrowserProcessData {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "E:\\hadoop2.6\\hadoop-2.6.3");
        args = new String[]{"--input-topic", "test1", "--bootstrap.servers", "192.168.71.13:9092",
                "--zookeeper.connect", "192.168.71.10:2181", "--group.id", "myconsumer1", "--winsdows.size", "50"};

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


        FlinkKafkaConsumer010 flinkKafkaConsumer = new FlinkKafkaConsumer010<KafkaMessage>(parameterTool.getRequired("input-topic"), new KafkaMessageSchema(), parameterTool.getProperties());
        DataStream<KafkaMessage> input = env.addSource(flinkKafkaConsumer.assignTimestampsAndWatermarks(new KafkaMessageWatermarks()));
        DataStream<UserBrowser> map = input.flatMap(new UserBrowserMap());
        DataStream<UserBrowser> reduce = map.keyBy("timestring").countWindow(Long.valueOf(parameterTool.getRequired("winsdows.size"))).reduce(new UserBrowserReduce());
//        reduce.print();
        reduce.addSink(new UserBrowsersinkreduce()).name("browserreduce");
        try {
            env.execute("browserfx");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

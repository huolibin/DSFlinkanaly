package com.haoxin.batch_ConsumptionLevel.task;

import com.haoxin.batch_ConsumptionLevel.map.ConsumptionlevelMap;
import com.haoxin.batch_ConsumptionLevel.reduce.ConsumptionlevelFinalReduce;
import com.haoxin.batch_ConsumptionLevel.reduce.ConsumptionlevelReduce;
import com.haoxin.batch_ConsumptionLevel.util.ConsumptionlevelInfo;
import com.haoxin.util.MongoUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;

import java.util.List;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/14 17:20
 * <p>
 * 对订单数据的统计
 * 通过平均消费金额来确定消费水平（高消费5000 中等消费 1000 低消费 小于1000）
 * 消费水平的标签写入Hbase
 * 最后的消费水平的汇总 写入Mongo
 */
public class ConsumptionlevelTask {
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);

        //source，get data
        DataSource<String> input = env.readTextFile(parameterTool.get("input"));
        //trans
        DataSet<ConsumptionlevelInfo> map = input.map(new ConsumptionlevelMap());//数据转换成ConsumptionlevelInfo格式
        DataSet<ConsumptionlevelInfo> reduceGroup = map.groupBy("groupField").reduceGroup(new ConsumptionlevelReduce());//通过对userid分组，对数据进行计算，并确定消费水平，写入hbase
        DataSet<ConsumptionlevelInfo> finalreduce = reduceGroup.groupBy("groupField").reduce(new ConsumptionlevelFinalReduce());//通过对消费水平分组，对数据进行计算，并确定消费水平汇总

        //sink 写入mongo
        try {
            List<ConsumptionlevelInfo> collect = finalreduce.collect();
            for (ConsumptionlevelInfo consumptionlevelInfo : collect) {
                String consumptionlevel = consumptionlevelInfo.getConsumptionlevel();
                long count = consumptionlevelInfo.getCount();
                Document doc = MongoUtil.findoneby("consumptionlevelstatics", "youfanPortrait", consumptionlevel);
                if (doc == null) {
                    doc = new Document();
                    doc.put("info", consumptionlevel);
                    doc.put("count", count);
                } else {
                    Long pre_count = doc.getLong("count");
                    doc.put("count", pre_count + count);
                }

                MongoUtil.saveorupdatemongo("consumptionlevelstatics", "youfanPortrait", doc);
            }

            env.execute("consumptionlevel analy");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

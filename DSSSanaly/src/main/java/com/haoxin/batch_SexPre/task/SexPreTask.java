package com.haoxin.batch_SexPre.task;

import com.haoxin.batch_SexPre.map.SexPreMap;
import com.haoxin.batch_SexPre.map.SexPreSaveMap;
import com.haoxin.batch_SexPre.reduce.SexPreReduce;
import com.haoxin.batch_SexPre.util.SexPreInfo;
import com.haoxin.batch_baijia.map.BaiJiaMap;
import com.haoxin.batch_baijia.reduce.BaiJiaReduce;
import com.haoxin.batch_baijia.util.BaiJiaInfo;
import com.haoxin.batch_baijia.util.DatetimeUtil;
import com.haoxin.util.HbaseUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.utils.ParameterTool;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/14 17:20
 * <p>
 * 回归预测sex
 *
 */
public class SexPreTask {
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);

        //source
        DataSource<String> input = env.readTextFile(parameterTool.get("input"));
        //trans
        DataSet<SexPreInfo> map = input.map(new SexPreMap());
        DataSet<ArrayList<Double>> groupfield = map.groupBy("groupfield").reduceGroup(new SexPreReduce());

        //计算
        try {
            List<ArrayList<Double>> collect = groupfield.collect();
            int groupsize = collect.size();
            Map<Integer, Double> summap = new TreeMap<>(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o1.compareTo(o2);
                }
            });

            for (ArrayList<Double> array:collect) {
                for (int i = 0; i < array.size(); i++) {
                    double pre = summap.get(i)==null?0d:summap.get(i);
                    summap.put(i,pre);
                }
            }
            ArrayList<Double> finalweight = new ArrayList<>();
            Set<Map.Entry<Integer, Double>> set = summap.entrySet();
            for (Map.Entry<Integer, Double> mapset:set
                 ) {
                Integer key = mapset.getKey();
                Double sumvalue = mapset.getValue();
                double finalvalue = sumvalue/groupsize;
                finalweight.add(finalvalue);
            }

            DataSource<String> text2 = env.readTextFile(parameterTool.get("input2"));
            text2.map(new SexPreSaveMap(finalweight));

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

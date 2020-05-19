package com.haoxin.stream_matrix.task;

import com.haoxin.stream_matrix.map.LogicMap;
import com.haoxin.stream_matrix.reduce.LogicReduce;
import com.haoxin.stream_matrix.util.LogicInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.*;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/19 14:56
 */
public class LogicTask {
    public static void main(String[] args) {

        final ParameterTool params = ParameterTool.fromArgs(args);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        //get input data
        DataSource<String> data = env.readTextFile(params.get("input"));
        DataSet<LogicInfo> map = data.map(new LogicMap());

        DataSet<ArrayList<Double>> result = map.groupBy("groupbyfield").reduceGroup(new LogicReduce());
        try {
            List<ArrayList<Double>> collect = result.collect();
            int size = collect.size();
            TreeMap<Integer, Double> summap = new TreeMap<>(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o1.compareTo(o2);
                }
            });
            for (ArrayList<Double> array:collect
                 ) {
                for (int i = 0; i < array.size(); i++) {
                   double pre = summap.get(i) == null?0d:summap.get(i);
                   summap.put(i,pre+array.get(i));
                }
            }
            ArrayList<Double> finalweight = new ArrayList<>();
            Set<Map.Entry<Integer, Double>> entries = summap.entrySet();
            for (Map.Entry<Integer, Double> set:entries
                 ) {
                Double value = set.getValue();
                Integer key = set.getKey();
                double finalvalue = value/size;
            }

            env.execute("logic fx");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

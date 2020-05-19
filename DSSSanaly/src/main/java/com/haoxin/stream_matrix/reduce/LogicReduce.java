package com.haoxin.stream_matrix.reduce;

import com.haoxin.stream_matrix.util.CreateDataSet;
import com.haoxin.stream_matrix.util.LogicInfo;
import com.haoxin.stream_matrix.util.Logistic;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/19 15:34
 */
public class LogicReduce implements GroupReduceFunction<LogicInfo, ArrayList<Double>> {
    @Override
    public void reduce(Iterable<LogicInfo> iterable, Collector<ArrayList<Double>> collector) throws Exception {
        Iterator<LogicInfo> iterator = iterable.iterator();
        CreateDataSet createDataSet = new CreateDataSet();
        while (iterator.hasNext()){
            LogicInfo next = iterator.next();
            String variable1 = next.getVariable1();
            String variable2 = next.getVariable2();
            String variable3 = next.getVariable3();
            String labase = next.getLabase();

            ArrayList<String> as = new ArrayList<>();
            as.add(variable1);
            as.add(variable2);
            as.add(variable3);

            createDataSet.data.add(as);
            createDataSet.labels.add(labase);
        }
        ArrayList<Double> weights = new ArrayList<>();
        weights = Logistic.gradAscent1(createDataSet, createDataSet.labels, 500);
        collector.collect(weights);
    }
}

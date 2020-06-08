package com.haoxin.batch_tfidf.task;

import com.haoxin.batch_order.map.ProductMap;
import com.haoxin.batch_order.reduce.ProductReduce;
import com.haoxin.batch_order.util.ProductAnaly;
import com.haoxin.batch_tfidf.map.IdfMap;
import com.haoxin.batch_tfidf.map.IdffinalMap;
import com.haoxin.batch_tfidf.reduce.IdfReduce;
import com.haoxin.batch_tfidf.util.TfIdfEntity;
import com.haoxin.util.HbaseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/12 12:24
 *
 * batch计算
 *
 */
public class tdidfTask {
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);

        //source
        DataSource<String> input = env.readTextFile(parameterTool.get("input"));
        //trans
        DataSet<TfIdfEntity> map = input.map(new IdfMap());
        DataSet<TfIdfEntity> reduce = map.reduce(new IdfReduce()).setParallelism(1);
        try {
            List<TfIdfEntity> collect = reduce.collect();
            Long totaldocumet = collect.get(0).getTotaldocumet();
            DataSet<TfIdfEntity> mapfinalresult = map.map(new IdffinalMap(totaldocumet, 4));
            mapfinalresult.writeAsText("hdfs://haoxin/test");//hdfs路径
            env.execute("batch_tfidf");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

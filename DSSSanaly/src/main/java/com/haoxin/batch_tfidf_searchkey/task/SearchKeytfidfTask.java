package com.haoxin.batch_tfidf_searchkey.task;

import com.haoxin.batch_tfidf.map.IdfMap;
import com.haoxin.batch_tfidf.map.IdffinalMap;
import com.haoxin.batch_tfidf.reduce.IdfReduce;
import com.haoxin.batch_tfidf.util.TfIdfEntity;
import com.haoxin.batch_tfidf_searchkey.map.SearchKeyMap;
import com.haoxin.batch_tfidf_searchkey.map.SearchKeyfinalMap;
import com.haoxin.batch_tfidf_searchkey.map.SearchkeyMap2;
import com.haoxin.batch_tfidf_searchkey.reduce.SearchKeyReduce;
import com.haoxin.batch_tfidf_searchkey.reduce.SearchKeyReduce2;
import com.haoxin.batch_tfidf_searchkey.util.SearchKeyTfIdfEntity;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.scala.map;

import java.util.List;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/8 18:24
 *
 *
 */
public class SearchKeytfidfTask {
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);

        //source
        DataSource<String> input = env.readTextFile(parameterTool.get("input"));
        //trans
        DataSet<SearchKeyTfIdfEntity> map = input.map(new SearchKeyMap());//对数据的切分，成为SearchKeyTfIdfEntity
        DataSet<SearchKeyTfIdfEntity> reduceresult1 = map.groupBy("userid").reduce(new SearchKeyReduce());//对相同userid 累加
        DataSet<SearchKeyTfIdfEntity> map2 = reduceresult1.map(new SearchkeyMap2());//对数据进行tf转换，并写入hbase
        DataSet<SearchKeyTfIdfEntity> reduceresult2 = map2.reduce(new SearchKeyReduce2()).setParallelism(1);//对数据docid的汇总


        try {
            List<SearchKeyTfIdfEntity> collect = reduceresult2.collect();
            Long totaldocumet = collect.get(0).getTotaldocumet();
            DataSet<SearchKeyTfIdfEntity> finalresult = reduceresult1.map(new SearchKeyfinalMap(totaldocumet, 3, "year"));
            finalresult.writeAsText("hdfs://haoxin/test/keyword/year");//hdfs路径
            env.execute("batch_tfidf_keyword analy");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

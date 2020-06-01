package com.haoxin.batch_kmeans.task;

import com.haoxin.batch_kmeans.map.KMeansMap;
import com.haoxin.batch_kmeans.map.KMeansfinalMap;
import com.haoxin.batch_kmeans.reduce.KMeansReduce;
import com.haoxin.batch_kmeans.util.KMeans;
import com.haoxin.kmeans.Cluster;
import com.haoxin.kmeans.KMeansRun;
import com.haoxin.kmeans.Point;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/12 12:24
 * <p>
 * KMeans的flink应用
 */
public class KMeansTask {
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);

        //source
        DataSource<String> input = env.readTextFile(parameterTool.get("input"));
        //trans
        DataSet<KMeans> map = input.map(new KMeansMap());
        DataSet<ArrayList<Point>> reduce = map.groupBy("groupbyfield").reduceGroup(new KMeansReduce());

        try {
            List<ArrayList<Point>> collect = reduce.collect();
            ArrayList<float[]> dataset = new ArrayList<float[]>();
            for (ArrayList<Point> array : collect) {
                for (Point p : array) {
                    dataset.add(p.getlocalArray());
                }
            }
            KMeansRun kMeansRun = new KMeansRun(6, dataset);
            Set<Cluster> clusterSet = kMeansRun.run();
            ArrayList<Point> flinalClutercenter = new ArrayList<>();
            int count = 100;
            for (Cluster c : clusterSet) {
                Point point = c.getCenter();
                point.setId(count++);
                flinalClutercenter.add(point);
            }

            DataSet<Point> finalmap = input.map(new KMeansfinalMap(flinalClutercenter));
            finalmap.writeAsText(parameterTool.get("out"));
            env.execute("batch_kmeans");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

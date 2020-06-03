package com.haoxin.batch_kmeans_usergroup.task;

import com.haoxin.batch_kmeans_usergroup.map.UserGroupKMeansfinalMap;
import com.haoxin.batch_kmeans_usergroup.map.UserGroupMap;
import com.haoxin.batch_kmeans_usergroup.map.UserGroupMapByReduce;
import com.haoxin.batch_kmeans_usergroup.reduce.UserGroupByKmeansReduce;
import com.haoxin.batch_kmeans_usergroup.reduce.UserGroupReduce;
import com.haoxin.batch_kmeans_usergroup.util.UserGroupInfo;
import com.haoxin.batch_kmeans_usergroup.util.UserGroupKMeansRun;
import com.haoxin.kmeans.Cluster;
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
public class UserGroupKmeansTask {
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);

        //source
        DataSource<String> input = env.readTextFile(parameterTool.get("input"));

        //trans
        DataSet<UserGroupInfo> map = input.map(new UserGroupMap());//对string数据转换成UserGroupInfo
        DataSet<UserGroupInfo> reduce = map.groupBy("groupbyfield").reduce(new UserGroupReduce());//对相同userid的数据迭代
        DataSet<UserGroupInfo> mapbyreduce = reduce.map(new UserGroupMapByReduce());//对数据计算出 平均消费 消费频次等指标
        DataSet<ArrayList<Point>> finalresult = mapbyreduce.groupBy("groupbyfield").reduceGroup(new UserGroupByKmeansReduce());//应用kmeans得到center


        try {
            List<ArrayList<Point>> collect = finalresult.collect();
            ArrayList<float[]> dataset = new ArrayList<float[]>();
            for (ArrayList<Point> array : collect) {
                for (Point p : array) {
                    dataset.add(p.getlocalArray());
                }
            }

            UserGroupKMeansRun userGroupKMeansRun = new UserGroupKMeansRun(6, dataset);
            Set<Cluster> clusterSet = userGroupKMeansRun.run();
            ArrayList<Point> flinalClutercenter = new ArrayList<>();
            int count = 100;
            for (Cluster c : clusterSet) {
                Point point = c.getCenter();
                point.setId(count++);
                flinalClutercenter.add(point);
            }

            DataSet<Point> map1 = mapbyreduce.map(new UserGroupKMeansfinalMap(flinalClutercenter));//计算出每个point的信息，结果输入到Hbase
            env.execute("batch_kmeans_usergroup analy");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

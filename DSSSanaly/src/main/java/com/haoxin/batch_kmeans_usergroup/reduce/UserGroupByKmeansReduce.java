package com.haoxin.batch_kmeans_usergroup.reduce;

import com.haoxin.batch_kmeans_usergroup.util.UserGroupInfo;
import com.haoxin.batch_kmeans_usergroup.util.UserGroupKMeansRun;
import com.haoxin.kmeans.Cluster;
import com.haoxin.kmeans.Point;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/3 10:20
 */
public class UserGroupByKmeansReduce implements GroupReduceFunction<UserGroupInfo, ArrayList<Point>> {
    @Override
    public void reduce(Iterable<UserGroupInfo> iterable, Collector<ArrayList<Point>> collector) throws Exception {
        Iterator<UserGroupInfo> iterator = iterable.iterator();
        ArrayList<float[]> dataset = new ArrayList<>();
        while (iterator.hasNext()) {
            UserGroupInfo userGroupInfo = iterator.next();
            float[] f = new float[]{Float.valueOf(userGroupInfo.getUserid()), Float.valueOf(userGroupInfo.getAvramout() + ""), Float.valueOf(userGroupInfo.getMaxamout() + ""), Float.valueOf(userGroupInfo.getDays() + ""),
                    Float.valueOf(userGroupInfo.getBuytype1() + ""), Float.valueOf(userGroupInfo.getBuytype2() + ""), Float.valueOf(userGroupInfo.getBuytype3() + ""),
                    Float.valueOf(userGroupInfo.getBuytime1() + ""), Float.valueOf(userGroupInfo.getBuytime2() + ""), Float.valueOf(userGroupInfo.getBuytime3() + ""), Float.valueOf(userGroupInfo.getBuytime4() + "")};
            dataset.add(f);
        }

        UserGroupKMeansRun userGroupKMeansRun = new UserGroupKMeansRun(6, dataset);
        ArrayList<Point> arrayList = new ArrayList<>();
        Set<Cluster> clusterSet = userGroupKMeansRun.run();
        for (Cluster cluster : clusterSet) {
            arrayList.add(cluster.getCenter());
        }
        collector.collect(arrayList);
    }
}

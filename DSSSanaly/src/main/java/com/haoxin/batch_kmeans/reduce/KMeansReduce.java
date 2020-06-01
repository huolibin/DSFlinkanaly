package com.haoxin.batch_kmeans.reduce;

import com.haoxin.batch_kmeans.util.KMeans;
import com.haoxin.kmeans.Cluster;
import com.haoxin.kmeans.KMeansRun;
import com.haoxin.kmeans.Point;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/12 14:37
 */
public class KMeansReduce implements GroupReduceFunction<KMeans, ArrayList<Point>> {
    @Override
    public void reduce(Iterable<KMeans> iterable, Collector<ArrayList<Point>> collector) throws Exception {
        Iterator<KMeans> kMeansIterator = iterable.iterator();
        ArrayList<float[]> dataset = new ArrayList<>();
        while (kMeansIterator.hasNext()) {
            KMeans next = kMeansIterator.next();
            float[] f = new float[]{Float.valueOf(next.getVariable1()), Float.valueOf(next.getVariable2()), Float.valueOf(next.getVariable3())};
            dataset.add(f);
        }

        KMeansRun kMeansRun = new KMeansRun(6, dataset);
        Set<Cluster> clusterSet = kMeansRun.run();

        ArrayList<Point> arrayList = new ArrayList<>();
        for (Cluster cluster : clusterSet
        ) {
            arrayList.add(cluster.getCenter());
        }

        collector.collect(arrayList);
    }
}

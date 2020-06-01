package com.haoxin.batch_kmeans.map;

import com.haoxin.kmeans.DistanceCompute;
import com.haoxin.kmeans.Point;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/12 12:26
 */
public class KMeansfinalMap implements MapFunction<String, Point> {

    private List<Point> centers = new ArrayList<Point>();
    private DistanceCompute disC = new DistanceCompute();


    public KMeansfinalMap(List<Point> centers) {
        this.centers = centers;
    }

    @Override
    public Point map(String s) throws Exception {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        //1,2,3
        String[] split = s.split(",");
        String variable1 = split[0];
        String variable2 = split[1];
        String variable3 = split[2];

        Point self = new Point(1, new float[]{Float.valueOf(variable1), Float.valueOf(variable2), Float.valueOf(variable3)});
        float min_dis = Integer.MAX_VALUE;
        for (Point point : centers) {
            float tmp_dis = (float) Math.min(disC.getEuclideanDis(self, point), min_dis);
            if (tmp_dis != min_dis) {
                min_dis = tmp_dis;
                self.setClusterId(point.getId());
                self.setDist(min_dis);
                self.setClusterPoint(point);
            }
        }

        return self;
    }
}

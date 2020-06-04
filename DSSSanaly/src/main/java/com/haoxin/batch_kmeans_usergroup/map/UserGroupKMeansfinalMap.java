package com.haoxin.batch_kmeans_usergroup.map;

import com.alibaba.fastjson.JSONObject;
import com.haoxin.batch_kmeans_usergroup.util.UserGroupInfo;
import com.haoxin.kmeans.DistanceCompute;
import com.haoxin.kmeans.Point;
import com.haoxin.util.HbaseUtil;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class UserGroupKMeansfinalMap implements MapFunction<UserGroupInfo, Point> {

    private List<Point> centers = new ArrayList<Point>();
    private DistanceCompute disC = new DistanceCompute();


    public UserGroupKMeansfinalMap(List<Point> centers) {
        this.centers = centers;
    }

    @Override
    public Point map(UserGroupInfo userGroupInfo) throws Exception {
        float[] f = new float[]{Float.valueOf(userGroupInfo.getAvramout() + ""), Float.valueOf(userGroupInfo.getMaxamout() + ""), Float.valueOf(userGroupInfo.getDays() + ""),
                Float.valueOf(userGroupInfo.getBuytype1() + ""), Float.valueOf(userGroupInfo.getBuytype2() + ""), Float.valueOf(userGroupInfo.getBuytype3() + ""),
                Float.valueOf(userGroupInfo.getBuytime1() + ""), Float.valueOf(userGroupInfo.getBuytime2() + ""), Float.valueOf(userGroupInfo.getBuytime3() + ""), Float.valueOf(userGroupInfo.getBuytime4() + "")};

        Point self = new Point(Integer.valueOf(userGroupInfo.getUserid()), f);
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

        String tablename = "userflaginfo";
        String rowkey = self.getId() + "";
        String famliyname = "usergroupinfo";
        String colum = "usergroupinfo";//用户分群信息
        HbaseUtil.putdata(tablename, rowkey, famliyname, colum, JSONObject.toJSONString(self));

        return self;
    }
}
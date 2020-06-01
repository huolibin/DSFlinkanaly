package com.haoxin.batch_kmeans.map;

import com.alibaba.fastjson.JSONObject;
import com.haoxin.batch_kmeans.util.KMeans;
import com.haoxin.batch_order.util.OrderInfo;
import com.haoxin.batch_order.util.ProductAnaly;
import com.haoxin.util.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Random;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/12 12:26
 *
 */
public class KMeansMap implements MapFunction<String, KMeans> {
    @Override
    public KMeans map(String s) throws Exception {
        if (StringUtils.isBlank(s)){
            return null;
        }
        //1,2,3
        String[] split = s.split(",");
        String variable1 = split[0];
        String variable2 = split[1];
        String variable3 = split[2];

        KMeans kMeans = new KMeans();
        kMeans.setVariable1(variable1);
        kMeans.setVariable2(variable2);
        kMeans.setVariable3(variable3);
        Random random = new Random();
        kMeans.setGroupbyfield("kmeans=="+random.nextInt(5));

        return kMeans;
    }
}

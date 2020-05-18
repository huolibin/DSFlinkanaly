package com.haoxin.stream_brandlike.map;

import com.alibaba.fastjson.JSONObject;
import com.haoxin.stream_brandlike.util.BrandLike;
import com.haoxin.stream_brandlike.util.KafkaEvent;
import com.haoxin.stream_brandlike.util.MapUtils;
import com.haoxin.stream_brandlike.util.ScanProductLog;
import com.haoxin.util.HbaseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/18 12:18
 */
public class BrandLikeMap implements FlatMapFunction<KafkaEvent, BrandLike> {
    @Override
    public void flatMap(KafkaEvent kafkaEvent, Collector<BrandLike> collector) throws Exception {
        String data = kafkaEvent.getWord();
        ScanProductLog scanProductLog = JSONObject.parseObject(data, ScanProductLog.class);
        String brand = scanProductLog.getBrand();
        int userid = scanProductLog.getUserid();

        String getdata = HbaseUtil.getdata("userflaginfo", userid+"", "userbehavior", "brandlist");
        Map<String, Long> map = new HashMap<>();
        if (StringUtils.isNoneBlank(getdata)){
            map = JSONObject.parseObject(getdata, Map.class);
        }
        //之前的品牌偏好
        String getpremaxbrand = MapUtils.getmaxbrand(map);

        long prebrand = map.get(brand)==null?0l:map.get(brand);
        map.put(brand,prebrand+1);
        HbaseUtil.putdata("userflaginfo", userid+"", "userbehavior", "brandlist",JSONObject.toJSONString(map));

        //现在的品牌偏好
        String getmaxbrand = MapUtils.getmaxbrand(map);

        //对两个偏好比较
        if (StringUtils.isNoneBlank(getmaxbrand)&&!getmaxbrand.equals(getpremaxbrand)){
            BrandLike brandLike = new BrandLike();
            brandLike.setBrand(getpremaxbrand);
            brandLike.setCount(-1l);
            brandLike.setGroupbyfield("brandlike="+getpremaxbrand);
            collector.collect(brandLike);
        }
        BrandLike brandLike = new BrandLike();
        brandLike.setBrand(getmaxbrand);
        brandLike.setCount(1l);
        brandLike.setGroupbyfield("brandlike="+getmaxbrand);
        collector.collect(brandLike);

        HbaseUtil.putdata("userflaginfo", userid+"", "userbehavior", "brandlike",getmaxbrand);
    }
}

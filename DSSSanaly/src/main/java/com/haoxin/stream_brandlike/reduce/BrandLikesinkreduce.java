package com.haoxin.stream_brandlike.reduce;

import com.haoxin.stream_brandlike.util.BrandLike;
import com.haoxin.util.MongoUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/7 17:02
 */
public class BrandLikesinkreduce implements SinkFunction<BrandLike> {
    @Override
    public void invoke(BrandLike value, Context context) throws Exception {
        String brand = value.getBrand();
        long count = value.getCount();
        Document doc = MongoUtil.findoneby("brandlikestatics", "youfanPortrait", brand);
        if (doc == null) {
            doc = new Document();
            doc.put("info", brand);
            doc.put("count", count);
        } else {
            Long countpre = doc.getLong("count");
            Long total = countpre + count;
            doc.put("count", total);
        }
        MongoUtil.saveorupdatemongo("brandlikestatics", "youfanPortrait", doc);
    }
}


package com.haoxin.stream_brandlike.reduce;

import com.haoxin.stream_brandlike.util.BrandLike;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/18 14:11
 */
public class BrandLikeReduce implements ReduceFunction<BrandLike> {
    @Override
    public BrandLike reduce(BrandLike brandLike, BrandLike t1) throws Exception {
        String brand = brandLike.getBrand();
        long count1 = brandLike.getCount();
        long count2 = t1.getCount();
        BrandLike brandLikefinal = new BrandLike();
        brandLikefinal.setBrand(brand);
        brandLikefinal.setCount(count1 + count2);
        return brandLikefinal;
    }
}

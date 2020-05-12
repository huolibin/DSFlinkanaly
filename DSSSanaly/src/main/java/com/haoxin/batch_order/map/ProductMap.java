package com.haoxin.batch_order.map;

import com.alibaba.fastjson.JSONObject;
import com.haoxin.batch_order.util.OrderInfo;
import com.haoxin.batch_order.util.ProductAnaly;
import com.haoxin.util.DateUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/12 12:26
 * 是对每个月的数据分类
 */
public class ProductMap implements FlatMapFunction<String, ProductAnaly> {
    @Override
    public void flatMap(String value, Collector<ProductAnaly> collector) throws Exception {
        OrderInfo orderInfo = JSONObject.parseObject(value, OrderInfo.class);
        long productid = orderInfo.getProductid();
        Date createtime = orderInfo.getCreatetime();
        String timestring = DateUtils.getDateby(createtime.getTime(), "yyyyMM");
        Date paytime = orderInfo.getPaytime();
        long chengjiaocount =0l;
        long nochjcount =0l;
        if (paytime != null){
            chengjiaocount=1l;
        }else {
            nochjcount =1l;
        }
        ProductAnaly productAnaly = new ProductAnaly();
        productAnaly.setProductid(productid);
        productAnaly.setChengjiaocount(chengjiaocount);
        productAnaly.setDateString(timestring);
        productAnaly.setNochjcount(nochjcount);
        productAnaly.setGroupbyfield(timestring+productid);

        collector.collect(productAnaly);
    }
}

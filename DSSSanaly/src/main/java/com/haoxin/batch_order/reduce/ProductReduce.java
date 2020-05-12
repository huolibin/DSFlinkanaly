package com.haoxin.batch_order.reduce;

import com.haoxin.batch_order.util.ProductAnaly;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/12 14:37
 */
public class ProductReduce implements ReduceFunction<ProductAnaly> {
    @Override
    public ProductAnaly reduce(ProductAnaly t1, ProductAnaly t2) throws Exception {
        long productid = t1.getProductid();
        long chengjiaocount1 = t1.getChengjiaocount();
        String dateString = t1.getDateString();
        long nochjcount1 = t1.getNochjcount();
        String groupbyfield = t1.getGroupbyfield();

        long nochjcount2 = t2.getNochjcount();
        long chengjiaocount2 = t2.getChengjiaocount();

        ProductAnaly productAnaly = new ProductAnaly();
        productAnaly.setProductid(productid);
        productAnaly.setGroupbyfield(groupbyfield);
        productAnaly.setNochjcount(nochjcount1 + nochjcount2);
        productAnaly.setDateString(dateString);
        productAnaly.setChengjiaocount(chengjiaocount1 + chengjiaocount2);

        return productAnaly;
    }
}

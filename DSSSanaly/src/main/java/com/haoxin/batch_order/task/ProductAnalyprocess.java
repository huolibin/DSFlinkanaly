package com.haoxin.batch_order.task;

import com.haoxin.batch_order.map.ProductMap;
import com.haoxin.batch_order.reduce.ProductReduce;
import com.haoxin.batch_order.util.ProductAnaly;
import com.haoxin.util.HbaseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/12 12:24
 *
 * batch计算
 * 计算订单每个月是否成功个数的统计
 */
public class ProductAnalyprocess {
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);

        //source
        DataSource<String> input = env.readTextFile(parameterTool.get("input"));
        //trans
        DataSet<ProductAnaly> map = input.flatMap(new ProductMap());
        DataSet<ProductAnaly> reduce = map.groupBy("groupbyfield").reduce(new ProductReduce());

        try {
            List<ProductAnaly> collect = reduce.collect();
            for (ProductAnaly p : collect
            ) {
                long productid = p.getProductid();
                String dateString = p.getDateString();
                long chengjiaocount = p.getChengjiaocount();
                long nochjcount = p.getNochjcount();
                String groupbyfield = p.getGroupbyfield();

                String hbasechengjiaocount = HbaseUtil.getdata("productinfo", groupbyfield, "info", "chengjiaocount");
                String hbasenochjcount = HbaseUtil.getdata("productinfo", groupbyfield, "info", "nochjcount");

                Map<String, String> datamap = new HashMap<>();
                if (StringUtils.isNoneBlank(hbasechengjiaocount)) {
                    chengjiaocount += Long.valueOf(hbasechengjiaocount);
                }

                if (StringUtils.isNoneBlank(hbasenochjcount)) {
                    nochjcount += Long.valueOf(hbasenochjcount);
                }

                datamap.put("chengjiaocount", chengjiaocount + "");
                datamap.put("nochjcount", nochjcount + "");

                HbaseUtil.put("productinfo", groupbyfield, "info", datamap);

            }

            env.execute("batch_product");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

package com.haoxin.batch_eamil.task;

import com.haoxin.batch_eamil.map.EmailMap;
import com.haoxin.batch_eamil.reduce.EmailReduce;
import com.haoxin.batch_eamil.util.EmailInfo;
import com.haoxin.batch_order.map.ProductMap;
import com.haoxin.batch_order.reduce.ProductReduce;
import com.haoxin.batch_order.util.ProductAnaly;
import com.haoxin.util.HbaseUtil;
import com.haoxin.util.MongoUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;

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
public class EmailAnalyprocess {
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);

        //source
        DataSource<String> input = env.readTextFile(parameterTool.get("input"));
        //trans
        DataSet<EmailInfo> map = input.map(new EmailMap());
        DataSet<EmailInfo> reduce = map.groupBy("groupfield").reduce(new EmailReduce());

        try {
            List<EmailInfo> collect = reduce.collect();
            for (EmailInfo p : collect
            ) {
                String emailtype = p.getEmailtype();
                long count = p.getCount();

                Document doc = MongoUtil.findoneby("eamilstatics", "yonghuhuaxiang", emailtype);//从mongo中查找
                if (doc == null){
                    doc = new Document();
                    doc.put("info",emailtype);
                    doc.put("count",count);
                }else {
                    Long countpre = doc.getLong("count");
                    Long total = count+countpre;
                    doc.put("count",total);
                }

                MongoUtil.saveorupdatemongo("eamilstatics", "yonghuhuaxiang",doc); //更新到mongo

            }

            env.execute("emailfx");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

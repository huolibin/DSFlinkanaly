package com.haoxin.batch_ConsumptionLevel.reduce;

import com.haoxin.batch_ConsumptionLevel.util.ConsumptionlevelInfo;
import com.haoxin.util.HbaseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/14 17:10
 * 通过平均消费来确定消费level
 */
public class ConsumptionlevelReduce implements GroupReduceFunction<ConsumptionlevelInfo, ConsumptionlevelInfo> {
    @Override
    public void reduce(Iterable<ConsumptionlevelInfo> iterable, Collector<ConsumptionlevelInfo> collector) throws Exception {
        Iterator<ConsumptionlevelInfo> iterator = iterable.iterator();
        int count = 0;
        double finaltotal = 0d;
        String userid = "-1";
        while (iterator.hasNext()) {
            ConsumptionlevelInfo consumptionlevelInfo = iterator.next();
            userid = consumptionlevelInfo.getUserid();
            double totalamount = consumptionlevelInfo.getTotalamount();
            finaltotal += totalamount;
            count++;
        }
        double avrmount = finaltotal / count;//高消费5000 中等消费 1000 低消费 小于1000
        String flag = "low";
        if (avrmount >= 1000 && avrmount < 5000) {
            flag = "middle";
        } else if (avrmount >= 5000) {
            flag = "high";
        }

        //获取Hbase中的数据
        String tablename = "userflaginfo";
        String rowkey = userid;
        String famliyname = "consumerinfo";
        String colum = "consumptionlevel";
        String getdata = HbaseUtil.getdata(tablename, rowkey, famliyname, colum);
        if (StringUtils.isBlank(getdata)) {
            ConsumptionlevelInfo consumptionlevelInfo1 = new ConsumptionlevelInfo();
            consumptionlevelInfo1.setUserid(userid);
            consumptionlevelInfo1.setConsumptionlevel(flag);
            consumptionlevelInfo1.setCount(1l);
            consumptionlevelInfo1.setGroupField("consumptionlevelfinal==" + flag);
            collector.collect(consumptionlevelInfo1);
        } else if (!getdata.equals(flag)) {
            ConsumptionlevelInfo consumptionlevelInfo2 = new ConsumptionlevelInfo();
            consumptionlevelInfo2.setUserid(userid);
            consumptionlevelInfo2.setConsumptionlevel(getdata);
            consumptionlevelInfo2.setCount(-1l);
            consumptionlevelInfo2.setGroupField("consumptionlevelfinal==" + getdata);

            ConsumptionlevelInfo consumptionlevelInfo3 = new ConsumptionlevelInfo();
            consumptionlevelInfo3.setUserid(userid);
            consumptionlevelInfo3.setConsumptionlevel(flag);
            consumptionlevelInfo3.setCount(1l);
            consumptionlevelInfo3.setGroupField("consumptionlevelfinal==" + flag);

            collector.collect(consumptionlevelInfo2);
            collector.collect(consumptionlevelInfo3);
        }
        HbaseUtil.putdata(tablename, rowkey, famliyname, colum, flag);//重新写入Hbase
    }
}

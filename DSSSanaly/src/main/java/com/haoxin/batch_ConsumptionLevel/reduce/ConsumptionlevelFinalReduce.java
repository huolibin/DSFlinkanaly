package com.haoxin.batch_ConsumptionLevel.reduce;

import com.haoxin.batch_ConsumptionLevel.util.ConsumptionlevelInfo;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/4 11:33
 * 对相同的type相加
 */
public class ConsumptionlevelFinalReduce implements ReduceFunction<ConsumptionlevelInfo> {
    @Override
    public ConsumptionlevelInfo reduce(ConsumptionlevelInfo value1, ConsumptionlevelInfo value2) throws Exception {
        String consumptionlevel = value1.getConsumptionlevel();
        Long count1 = value1.getCount();
        Long count2 = value2.getCount();

        ConsumptionlevelInfo consumptionlevelInfo = new ConsumptionlevelInfo();
        consumptionlevelInfo.setConsumptionlevel(consumptionlevel);
        consumptionlevelInfo.setCount(count1 + count2);
        return consumptionlevelInfo;
    }
}

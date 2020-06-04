package com.haoxin.stream_chaomanchaonv.reduce;

import com.haoxin.stream_chaomanchaonv.util.ChaoManAndWomanInfo;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/3 16:39
 */
public class ChaoManAndWomanfinalReduce implements ReduceFunction<ChaoManAndWomanInfo> {

    @Override
    public ChaoManAndWomanInfo reduce(ChaoManAndWomanInfo value1, ChaoManAndWomanInfo value2) throws Exception {

        String chaotype = value1.getChaotype();
        long count1 = value1.getCount();
        long count2 = value1.getCount();
        ChaoManAndWomanInfo chaoManAndWomanInfo = new ChaoManAndWomanInfo();
        chaoManAndWomanInfo.setChaotype(chaotype);
        chaoManAndWomanInfo.setCount(count1 + count2);
        return chaoManAndWomanInfo;
    }
}


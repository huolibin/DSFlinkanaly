package com.haoxin.stream_chaomanchaonv.reduce;

import com.haoxin.stream_chaomanchaonv.util.ChaoManAndWomanInfo;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.List;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/3 16:39
 */
public class ChaoManAndWomanReduce implements ReduceFunction<ChaoManAndWomanInfo> {

    @Override
    public ChaoManAndWomanInfo reduce(ChaoManAndWomanInfo value1, ChaoManAndWomanInfo value2) throws Exception {

        String userid = value1.getUserid();
        List<ChaoManAndWomanInfo> list1 = value1.getList();
        List<ChaoManAndWomanInfo> list2 = value2.getList();
        list1.addAll(list2);
        ChaoManAndWomanInfo chaoManAndWomanInfo = new ChaoManAndWomanInfo();
        chaoManAndWomanInfo.setUserid(userid);
        chaoManAndWomanInfo.setList(list1);
        return chaoManAndWomanInfo;
    }
}


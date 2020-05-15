package com.haoxin.batch_baijia.reduce;

import com.haoxin.batch_baijia.util.BaiJiaInfo;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/14 17:10
 * <p>
 * 通过reduce对baijiaindo 聚集在list中
 */
public class BaiJiaReduce implements ReduceFunction<BaiJiaInfo> {
    @Override
    public BaiJiaInfo reduce(BaiJiaInfo t1, BaiJiaInfo t2) throws Exception {
        String userid = t1.getUserid();
        List<BaiJiaInfo> list1 = t1.getList();
        List<BaiJiaInfo> list2 = t2.getList();
        ArrayList<BaiJiaInfo> list3 = new ArrayList<>();
        list3.addAll(list1);
        list3.addAll(list2);

        BaiJiaInfo baiJiaInfo = new BaiJiaInfo();
        baiJiaInfo.setUserid(userid);
        baiJiaInfo.setList(list3);
        return baiJiaInfo;
    }
}

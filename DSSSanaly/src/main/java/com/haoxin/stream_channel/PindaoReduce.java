package com.haoxin.stream_channel;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/4/21 17:04
 */
public class PindaoReduce implements org.apache.flink.api.common.functions.ReduceFunction<PindaoHost> {
    @Override
    public PindaoHost reduce(PindaoHost value1, PindaoHost value2) throws Exception {
        PindaoHost host = new PindaoHost();
        System.out.println("value1=="+value1);
        System.out.println("value2=="+value2);
        host.setPindaoid(value1.getPindaoid());
        host.setCount(value1.getCount()+value2.getCount());
        return host;
    }
}

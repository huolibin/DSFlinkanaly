package com.haoxin.stream_network;

import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/7 17:01
 */
public class UserNetworkReduce implements ReduceFunction<UserNetwork> {

    @Override
    public UserNetwork reduce(UserNetwork value1, UserNetwork value2) throws Exception {
        System.out.println("value1==" + value1);
        System.out.println("value2==" + value2);
        String network = value1.getNetwork();
        long timestampvalue = value1.getTimestamp();
        String timestring = value1.getTimestring();
        long count1 = value1.getCount();
        long newcount1 = value1.getNewcount();
        long oldcount1 = value1.getOldcount();

        long count2 = value2.getCount();
        long newcount2 = value2.getNewcount();
        long oldcount2 = value2.getOldcount();


        UserNetwork userNetwork = new UserNetwork();
        userNetwork.setNetwork(network);
        userNetwork.setTimestamp(timestampvalue);
        userNetwork.setTimestring(timestring);
        userNetwork.setCount(count1 + count2);
        userNetwork.setNewcount(newcount1 + newcount2);
        userNetwork.setOldcount(oldcount1 + oldcount2);
        System.out.println("recuduce --pidaoPvUv==" + userNetwork);
        return userNetwork;
    }
}


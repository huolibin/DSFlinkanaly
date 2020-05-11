package com.haoxin.stream_network;

import com.alibaba.fastjson.JSONObject;
import com.haoxin.util.HbaseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/7 17:02
 */
public class UserNetworksinkreduce implements SinkFunction<UserNetwork> {
    @Override
    public void invoke(UserNetwork value, Context context) throws Exception {
        System.out.println("recuducesinkd --userNetWork==" + value);
        String network = value.getNetwork();
        String timestring = value.getTimestring();
        long count = value.getCount();
        long newcount = value.getNewcount();
        long oldcount = value.getOldcount();


        String networkcount = HbaseUtil.getdata("userinfo", timestring, "info", "networkcount");
        String networknewcount = HbaseUtil.getdata("userinfo", timestring, "info", "networknewcount");
        String networkoldcount = HbaseUtil.getdata("userinfo", timestring, "info", "networkoldcount");

        Map<String, String> datamap = new HashMap<String, String>();

        Map<String, Long> map = new HashMap<>();

        /**
         * count
         */
        if (StringUtils.isNotBlank(networkcount)) {
            map = JSONObject.parseObject(networkcount, Map.class);
            Long aLong = map.get(network);
            if (aLong != null) {
                count += aLong;
            }
        }
        map.put(network, count);
        datamap.put("networkcount", JSONObject.toJSONString(map));

/**
 * newcount
 */
        if (StringUtils.isNotBlank(networknewcount)) {
            map = JSONObject.parseObject(networknewcount, Map.class);
            Long aLong = map.get(network);
            if (aLong != null) {
                newcount += aLong;
            }
        }
        map.put(network, newcount);
        datamap.put("networknewcount", JSONObject.toJSONString(map));

        /**
         * oldcount
         */
        if (StringUtils.isNotBlank(networkoldcount)) {
            map = JSONObject.parseObject(networkoldcount, Map.class);
            Long aLong = map.get(network);
            if (aLong != null) {
                oldcount += aLong;
            }
        }
        map.put(network, oldcount);
        datamap.put("networkoldcount", JSONObject.toJSONString(map));

        System.out.println("network---- HbaseUtil.put(network+" + "," + network + ",rowbey" + timestring + ",info" + datamap + ")");
        HbaseUtil.put("userinfo", timestring, "info", datamap);

    }
}


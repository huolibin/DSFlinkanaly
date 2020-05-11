package com.haoxin.stream_browser;

import com.alibaba.fastjson.JSONObject;
import com.haoxin.stream_network.UserNetwork;
import com.haoxin.util.HbaseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/11 17:02
 */
public class UserBrowsersinkreduce implements SinkFunction<UserBrowser> {
    @Override
    public void invoke(UserBrowser value, Context context) throws Exception {
        System.out.println("recuducesinkd --userBrowser==" + value);
        String browser = value.getBrowser();
        String timestring = value.getTimestring();
        long count = value.getCount();
        long newcount = value.getNewcount();
        long oldcount = value.getOldcount();


        String browsercount = HbaseUtil.getdata("userinfo", timestring, "info", "browsercount");
        String browsernewcount = HbaseUtil.getdata("userinfo", timestring, "info", "browsernewcount");
        String browseroldcount = HbaseUtil.getdata("userinfo", timestring, "info", "browseroldcount");

        Map<String, String> datamap = new HashMap<String, String>();

        Map<String, Long> map = new HashMap<>();

        /**
         * count
         */
        if (StringUtils.isNotBlank(browsercount)) {
            map = JSONObject.parseObject(browsercount, Map.class);
            Long aLong = map.get(browser);
            if (aLong != null) {
                count += aLong;
            }
        }
        map.put(browser, count);
        datamap.put("browsercount", JSONObject.toJSONString(map));

/**
 * newcount
 */
        if (StringUtils.isNotBlank(browsernewcount)) {
            map = JSONObject.parseObject(browsernewcount, Map.class);
            Long aLong = map.get(browser);
            if (aLong != null) {
                newcount += aLong;
            }
        }
        map.put(browser, newcount);
        datamap.put("browsernewcount", JSONObject.toJSONString(map));

        /**
         * oldcount
         */
        if (StringUtils.isNotBlank(browseroldcount)) {
            map = JSONObject.parseObject(browseroldcount, Map.class);
            Long aLong = map.get(browser);
            if (aLong != null) {
                oldcount += aLong;
            }
        }
        map.put(browser, oldcount);
        datamap.put("networkoldcount", JSONObject.toJSONString(map));

        System.out.println("browser---- HbaseUtil.put(browser+" + "," + browser + ",rowkey" + timestring + ",info" + datamap + ")");
        HbaseUtil.put("userinfo", timestring, "info", datamap);

    }
}


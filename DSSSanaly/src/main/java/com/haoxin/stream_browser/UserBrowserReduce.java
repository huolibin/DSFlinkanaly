package com.haoxin.stream_browser;

import com.haoxin.stream_network.UserNetwork;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/11 17:01
 */
public class UserBrowserReduce implements ReduceFunction<UserBrowser> {

    @Override
    public UserBrowser reduce(UserBrowser value1, UserBrowser value2) throws Exception {
        System.out.println("value1==" + value1);
        System.out.println("value2==" + value2);
        String browser = value1.getBrowser();
        long timestampvalue = value1.getTimestamp();
        String timestring = value1.getTimestring();
        long count1 = value1.getCount();
        long newcount1 = value1.getNewcount();
        long oldcount1 = value1.getOldcount();

        long count2 = value2.getCount();
        long newcount2 = value2.getNewcount();
        long oldcount2 = value2.getOldcount();


        UserBrowser userBrowser = new UserBrowser();
        userBrowser.setBrowser(browser);
        userBrowser.setTimestamp(timestampvalue);
        userBrowser.setTimestring(timestring);
        userBrowser.setCount(count1 + count2);
        userBrowser.setNewcount(newcount1 + newcount2);
        userBrowser.setOldcount(oldcount1 + oldcount2);
        System.out.println("recuduce --pidaoPvUv==" + userBrowser);
        return userBrowser;
    }
}


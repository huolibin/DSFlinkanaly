package com.haoxin.stream_browser;

import com.alibaba.fastjson.JSON;
import com.haoxin.log.KafkaMessage;
import com.haoxin.log.Userscanlog;
import com.haoxin.stream_network.UserNetwork;
import com.haoxin.stream_pvuv.PdvisterDao;
import com.haoxin.stream_pvuv.UserState;
import com.haoxin.util.DateUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/7 16:56
 */
public class UserBrowserMap implements FlatMapFunction<KafkaMessage, UserBrowser> {

    @Override
    public void flatMap(KafkaMessage value, Collector<UserBrowser> out) throws Exception {
        String jsonstring = value.getJsonmessage();
        long timestamp = value.getTimestamp();

        String hourtimestamp = DateUtils.getDateby(timestamp, "yyyyMMddhh");//小时
        String daytimestamp = DateUtils.getDateby(timestamp, "yyyyMMdd");//天
        String monthtimestamp = DateUtils.getDateby(timestamp, "yyyyMM");//月

        Userscanlog userscanLog = JSON.parseObject(jsonstring, Userscanlog.class);
        long userid = userscanLog.getYonghuids();
        String browser = userscanLog.getLiulanqitype();

        UserState userState = PdvisterDao.getUserSatebyvistertime(userid + "", timestamp);
        boolean isnew = userState.isnew();
        boolean isFirsthour = userState.isFisrthour();
        boolean isFisrtday = userState.isFisrtday();
        boolean isFisrtmonth = userState.isFisrtmonth();

        UserBrowser userBrowser = new UserBrowser();
        userBrowser.setBrowser(browser);
        userBrowser.setCount(1l);
        userBrowser.setTimestamp(timestamp);

        long newuser = 0l;
        if (isnew) {
            newuser = 1l;
        }
        userBrowser.setNewcount(newuser);

        //小时
        long olduser = 0l;
        if (isFirsthour) {
            olduser = 1l;
        }
        userBrowser.setOldcount(olduser);
        userBrowser.setTimestring(hourtimestamp);
        out.collect(userBrowser);
        System.out.println("小时==" + userBrowser);

        //天
        olduser = 0l;
        if (isFisrtday) {
            olduser = 1l;
        }
        userBrowser.setOldcount(olduser);
        userBrowser.setTimestring(daytimestamp);
        out.collect(userBrowser);
        System.out.println("天==" + userBrowser);

        //月
        olduser = 0l;
        if (isFisrtmonth) {
            olduser = 1l;
        }
        userBrowser.setOldcount(olduser);
        userBrowser.setTimestring(monthtimestamp);
        out.collect(userBrowser);
        System.out.println("月==" + userBrowser);

    }
}

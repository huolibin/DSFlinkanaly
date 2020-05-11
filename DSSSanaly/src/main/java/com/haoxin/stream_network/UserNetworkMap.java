package com.haoxin.stream_network;

import com.alibaba.fastjson.JSON;
import com.haoxin.log.KafkaMessage;
import com.haoxin.log.Userscanlog;
import com.haoxin.stream_pvuv.PdvisterDao;
import com.haoxin.stream_pvuv.UserState;
import com.haoxin.util.DateUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/7 16:56
 */
public class UserNetworkMap implements FlatMapFunction<KafkaMessage, UserNetwork> {

    @Override
    public void flatMap(KafkaMessage value, Collector<UserNetwork> out) throws Exception {
        String jsonstring = value.getJsonmessage();
        long timestamp = value.getTimestamp();

        String hourtimestamp = DateUtils.getDateby(timestamp, "yyyyMMddhh");//小时
        String daytimestamp = DateUtils.getDateby(timestamp, "yyyyMMdd");//天
        String monthtimestamp = DateUtils.getDateby(timestamp, "yyyyMM");//月

        Userscanlog userscanLog = JSON.parseObject(jsonstring, Userscanlog.class);
        long userid = userscanLog.getYonghuids();
        String network = userscanLog.getNetwork();

        UserState userState = PdvisterDao.getUserSatebyvistertime(userid + "", timestamp);
        boolean isnew = userState.isnew();
        boolean isFirsthour = userState.isFisrthour();
        boolean isFisrtday = userState.isFisrtday();
        boolean isFisrtmonth = userState.isFisrtmonth();

        UserNetwork userNetwork = new UserNetwork();
        userNetwork.setNetwork(network);
        userNetwork.setCount(1l);
        userNetwork.setTimestamp(timestamp);

        long newuser = 0l;
        if (isnew) {
            newuser = 1l;
        }
        userNetwork.setNewcount(newuser);

        //小时
        long olduser = 0l;
        if (isFirsthour) {
            olduser = 1l;
        }
        userNetwork.setOldcount(olduser);
        userNetwork.setTimestring(hourtimestamp);
        out.collect(userNetwork);
        System.out.println("小时==" + userNetwork);

        //天
        olduser = 0l;
        if (isFisrtday) {
            olduser = 1l;
        }
        userNetwork.setOldcount(olduser);
        userNetwork.setTimestring(daytimestamp);
        out.collect(userNetwork);
        System.out.println("天==" + userNetwork);

        //月
        olduser = 0l;
        if (isFisrtmonth) {
            olduser = 1l;
        }
        userNetwork.setOldcount(olduser);
        userNetwork.setTimestring(monthtimestamp);
        out.collect(userNetwork);
        System.out.println("月==" + userNetwork);

    }
}

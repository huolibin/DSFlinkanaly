package com.haoxin.stream_pvuv;

import com.alibaba.fastjson.JSON;
import com.haoxin.log.KafkaMessage;
import com.haoxin.log.Userscanlog;
import com.haoxin.util.DateUtils;
import org.apache.commons.httpclient.util.DateUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/7 16:56
 */
public class PindaopvuvMap implements FlatMapFunction<KafkaMessage,PidaoPvUv> {

    @Override
    public void flatMap(KafkaMessage value, Collector<PidaoPvUv> out) throws Exception {
        String jsonstring = value.getJsonmessage();
        long timestamp = value.getTimestamp();


        String hourtimestamp = DateUtils.getDateby(timestamp,"yyyyMMddhh");//小时
        String daytimestamp = DateUtils.getDateby(timestamp,"yyyyMMdd");//天
        String monthtimestamp = DateUtils.getDateby(timestamp,"yyyyMM");//月

        Userscanlog userscanLog = JSON.parseObject(jsonstring, Userscanlog.class);
        long pingdaoid = userscanLog.getPindaoids();
        long userid = userscanLog.getYonghuids();

        UserState userState = PdvisterDao.getUserSatebyvistertime(userid+"",timestamp);
        boolean isFirsthour = userState.isFisrthour();
        boolean isFisrtday = userState.isFisrtday();
        boolean isFisrtmonth = userState.isFisrtmonth();

        //小时
        PidaoPvUv pidaoPvUv = new PidaoPvUv();
        pidaoPvUv.setPingdaoid(pingdaoid);
        pidaoPvUv.setUserid(userid);
        pidaoPvUv.setPvcount(Long.valueOf(value.getCount()+""));
        pidaoPvUv.setUvcount(isFirsthour==true?1l:0l);
        pidaoPvUv.setTimestamp(timestamp);
        pidaoPvUv.setTimestring(hourtimestamp);
        pidaoPvUv.setGroupbyfield(hourtimestamp+pingdaoid);
        out.collect(pidaoPvUv);
        System.out.println("小时=="+pidaoPvUv);

        //天
        pidaoPvUv.setUvcount(isFisrtday==true?1l:0l);
        pidaoPvUv.setGroupbyfield(daytimestamp+pingdaoid);
        pidaoPvUv.setTimestring(daytimestamp);
        out.collect(pidaoPvUv);
        System.out.println("天=="+pidaoPvUv);
        //月
        pidaoPvUv.setUvcount(isFisrtmonth==true?1l:0l);
        pidaoPvUv.setGroupbyfield(monthtimestamp+pingdaoid);
        pidaoPvUv.setTimestring(monthtimestamp);
        out.collect(pidaoPvUv);
        System.out.println("月=="+pidaoPvUv);
    }
}

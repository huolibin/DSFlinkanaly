package com.haoxin.stream_xinxiandu;

import com.alibaba.fastjson.JSON;
import com.haoxin.log.KafkaMessage;
import com.haoxin.log.Userscanlog;
import com.haoxin.stream_pvuv.PdvisterDao;
import com.haoxin.stream_pvuv.PidaoPvUv;
import com.haoxin.stream_pvuv.UserState;
import com.haoxin.util.DateUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/7 16:56
 * 对数据进行map
 */
public class PindaoXinXianDuMap implements FlatMapFunction<KafkaMessage,PindaoXinXianDu> {

    @Override
    public void flatMap(KafkaMessage value, Collector<PindaoXinXianDu> out) throws Exception {
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
        boolean isnew = userState.isnew();

        PindaoXinXianDu pindaoXinXianDu = new PindaoXinXianDu();
        pindaoXinXianDu.setPindaoid(pingdaoid);
        pindaoXinXianDu.setTimestamp(timestamp);

        long newuser =0l;
        if (isnew){
            newuser = 1l;
        }
        pindaoXinXianDu.setNewcount(newuser);

        //小时
        long olduser =0l;
        if (!isnew&&isFirsthour){
            olduser=1l;
        }
        pindaoXinXianDu.setOldcount(olduser);
        pindaoXinXianDu.setTimestring(hourtimestamp);
        pindaoXinXianDu.setGroupbyfield(hourtimestamp+pingdaoid);
        out.collect(pindaoXinXianDu);
        System.out.println("小时=="+pindaoXinXianDu);

        //天
        olduser =0l;
        if (!isnew&&isFisrtday){
            olduser=1l;
        }
        pindaoXinXianDu.setOldcount(olduser);
        pindaoXinXianDu.setTimestring(daytimestamp);
        pindaoXinXianDu.setGroupbyfield(daytimestamp+pingdaoid);
        out.collect(pindaoXinXianDu);
        System.out.println("天=="+pindaoXinXianDu);
        //月
        olduser =0l;
        if (!isnew&&isFisrtmonth){
            olduser=1l;
        }
        pindaoXinXianDu.setOldcount(olduser);
        pindaoXinXianDu.setTimestring(monthtimestamp);
        pindaoXinXianDu.setGroupbyfield(monthtimestamp+pingdaoid);
        out.collect(pindaoXinXianDu);
        System.out.println("月=="+pindaoXinXianDu);
    }
}

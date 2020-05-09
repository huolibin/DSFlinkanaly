package com.haoxin.stream_area;

import com.alibaba.fastjson.JSON;
import com.haoxin.log.KafkaMessage;
import com.haoxin.log.Userscanlog;
import com.haoxin.stream_pvuv.PdvisterDao;
import com.haoxin.stream_pvuv.UserState;
import com.haoxin.stream_xinxiandu.PindaoXinXianDu;
import com.haoxin.util.DateUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/7 16:56
 * 对数据进行map
 */
public class PindaoAreaMap implements FlatMapFunction<KafkaMessage,PindaoArea> {

    @Override
    public void flatMap(KafkaMessage value, Collector<PindaoArea> out) throws Exception {
        String jsonstring = value.getJsonmessage();
        long timestamp = value.getTimestamp();


        String hourtimestamp = DateUtils.getDateby(timestamp,"yyyyMMddhh");//小时
        String daytimestamp = DateUtils.getDateby(timestamp,"yyyyMMdd");//天
        String monthtimestamp = DateUtils.getDateby(timestamp,"yyyyMM");//月

        Userscanlog userscanLog = JSON.parseObject(jsonstring, Userscanlog.class);
        long pingdaoid = userscanLog.getPindaoids();
        long userid = userscanLog.getYonghuids();
        String city = userscanLog.getCity();

        UserState userState = PdvisterDao.getUserSatebyvistertime(userid+"",timestamp);
        boolean isFirsthour = userState.isFisrthour();
        boolean isFisrtday = userState.isFisrtday();
        boolean isFisrtmonth = userState.isFisrtmonth();
        boolean isnew = userState.isnew();

        PindaoArea pindaoArea = new PindaoArea();
        pindaoArea.setPindaoid(pingdaoid);
        pindaoArea.setTimestamp(timestamp);
        pindaoArea.setArea(city);

        pindaoArea.setPv(1l);//pv
        long newuser =0l;
        if (isnew){
            newuser = 1l;
        }
        pindaoArea.setNewcount(newuser);//newcount

        //小时
        long uvcount=0l;
        long olduser =0l;
        if (!isnew&&isFirsthour){
            olduser=1l;
            uvcount=1l;
        }
        pindaoArea.setUv(uvcount);
        pindaoArea.setOldcount(olduser);
        pindaoArea.setTimestring(hourtimestamp);
        pindaoArea.setGroupbyfield(hourtimestamp+pingdaoid);
        out.collect(pindaoArea);
        System.out.println("小时=="+pindaoArea);

        //天
        uvcount=0l;
        olduser =0l;
        if (!isnew&&isFisrtday){
            olduser=1l;
            uvcount=1l;
        }
        pindaoArea.setUv(uvcount);
        pindaoArea.setOldcount(olduser);
        pindaoArea.setTimestring(daytimestamp);
        pindaoArea.setGroupbyfield(daytimestamp+pingdaoid);
        out.collect(pindaoArea);
        System.out.println("天=="+pindaoArea);

        //月
        uvcount=0l;
        olduser =0l;
        if (!isnew&&isFirsthour){
            olduser=1l;
            uvcount=1l;
        }
        pindaoArea.setUv(uvcount);
        pindaoArea.setOldcount(olduser);
        pindaoArea.setTimestring(monthtimestamp);
        pindaoArea.setGroupbyfield(monthtimestamp+pingdaoid);
        out.collect(pindaoArea);
        System.out.println("月=="+pindaoArea);
    }
}

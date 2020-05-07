package com.haoxin.stream_pvuv;

import com.haoxin.util.HbaseUtil;
import org.apache.commons.lang3.StringUtils;
import com.haoxin.util.DateUtils;
import java.util.HashMap;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/7 16:43
 */
public class PdvisterDao {
    /**
     * 查询本次用户的访问状态
     * @param userid
     * @param timestamp
     * @return
     */
    public static UserState getUserSatebyvistertime(String userid,long timestamp){
        UserState userState = new UserState();
        try {
            String result =  HbaseUtil.getdata("baseuserscaninfo",userid,"time","fisrtvisittime");
            if(result == null){//第一次访问
                HashMap<String, String> datamap = new HashMap<>();
                datamap.put("fisrtvisittime",timestamp+"");
                datamap.put("lastvisittime",timestamp+"");
                HbaseUtil.put("baseuserscaninfo",userid,"time",datamap);
                userState.setIsnew(true);
                userState.setFisrtday(true);
                userState.setFisrthour(true);
                userState.setFisrtmonth(true);
            }else{
                String lastvisittimestring = HbaseUtil.getdata("baseuserscaninfo",  userid, "time","lastvisittime");
                if(StringUtils.isNotBlank(lastvisittimestring)){
                    long lastvisittime = Long.valueOf(lastvisittimestring);
                    //小时
                    long timstamp = DateUtils.getDatebyConditon(timestamp,"yyyyMMddhh");
                    if(lastvisittime < timestamp){
                        userState.setFisrthour(true);
                    }
                    //天
                    timstamp = DateUtils.getDatebyConditon(timestamp,"yyyyMMdd");
                    if(lastvisittime < timestamp){
                        userState.setFisrtday(true);
                    }
                    //月
                    timstamp = DateUtils.getDatebyConditon(timestamp,"yyyyMM");
                    if(lastvisittime < timestamp){
                        userState.setFisrtmonth(true);
                    }
                }
                HbaseUtil.putdata("baseuserscaninfo", userid, "time","lastvisittime",timestamp+"");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return userState;
    }
}

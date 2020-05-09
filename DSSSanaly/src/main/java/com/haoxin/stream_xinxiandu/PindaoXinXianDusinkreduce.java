package com.haoxin.stream_xinxiandu;

import com.haoxin.stream_pvuv.PidaoPvUv;
import com.haoxin.util.HbaseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/7 17:02
 */
public class PindaoXinXianDusinkreduce implements SinkFunction<PindaoXinXianDu> {
    @Override
    public void invoke(PindaoXinXianDu value, Context context) throws Exception {
        System.out.println( "recuducesinkd --pidaoXinXianDu=="+value);
        long pingdaoid = value.getPindaoid();
        long newcount = value.getNewcount();
        long oldcount = value.getOldcount();
        String timestring = value.getTimestring();
        String newcountstr = HbaseUtil.getdata("pindaoinfo",pingdaoid+timestring,"info","xinxiandunew");
        String oldcountstr = HbaseUtil.getdata("pindaoinfo",pingdaoid+timestring,"info","xinxianduold");
        if(StringUtils.isNotBlank(newcountstr)){
            newcount += newcount + Long.valueOf(newcountstr);
        }
        if(StringUtils.isNotBlank(oldcountstr)) {
            oldcountstr += oldcountstr + Long.valueOf(oldcountstr);
        }

        Map<String,String> datamap = new HashMap<String,String>();
        datamap.put("xinxiandunew",newcount+"");
        datamap.put("xinxianduold",oldcount+"");
        System.out.println( "pidaopvuv---- HbaseUtil.put(pindaoinfo+"+","+pingdaoid+timestring+",info"+datamap+")");
        HbaseUtil.put("pindaoinfo",pingdaoid+timestring,"info",datamap);

    }
}


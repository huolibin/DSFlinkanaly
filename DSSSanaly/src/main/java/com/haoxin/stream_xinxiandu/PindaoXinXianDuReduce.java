package com.haoxin.stream_xinxiandu;

import com.haoxin.stream_pvuv.PidaoPvUv;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/7 17:01
 */
public class PindaoXinXianDuReduce implements ReduceFunction<PindaoXinXianDu> {

    @Override
    public PindaoXinXianDu reduce(PindaoXinXianDu value1,PindaoXinXianDu value2) throws Exception {
        System.out.println( "value1=="+value1);
        System.out.println( "value2=="+value2);
        long pingdaoid = value1.getPindaoid();
        long timestampvalue = value1.getTimestamp();
        String timestring = value1.getTimestring();
        long newcountvalue1 = value1.getNewcount();
        long oldcountvalue1 = value1.getOldcount();

        long newcountvalue2 = value2.getNewcount();
        long oldcountvalue2 = value2.getOldcount();

        PindaoXinXianDu pindaoXinXianDu = new PindaoXinXianDu();
        pindaoXinXianDu.setPindaoid(pingdaoid);
        pindaoXinXianDu.setTimestamp(timestampvalue);
        pindaoXinXianDu.setTimestring(timestring);
        pindaoXinXianDu.setNewcount(newcountvalue1+newcountvalue2);
        pindaoXinXianDu.setOldcount(oldcountvalue1+oldcountvalue2);
        System.out.println( "recuduce --pidaoXinXiandu=="+pindaoXinXianDu);
        return  pindaoXinXianDu;
    }
}


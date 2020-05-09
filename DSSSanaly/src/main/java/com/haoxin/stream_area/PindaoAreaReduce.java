package com.haoxin.stream_area;

import com.haoxin.stream_xinxiandu.PindaoXinXianDu;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/7 17:01
 */
public class PindaoAreaReduce implements ReduceFunction<PindaoArea> {

    @Override
    public PindaoArea reduce(PindaoArea value1,PindaoArea value2) throws Exception {
        System.out.println( "value1=="+value1);
        System.out.println( "value2=="+value2);
        long pingdaoid = value1.getPindaoid();
        long timestampvalue = value1.getTimestamp();
        String timestring = value1.getTimestring();
        String area = value1.getArea();
        long pv1 = value1.getPv();
        long uv1 = value1.getUv();
        long newcountvalue1 = value1.getNewcount();
        long oldcountvalue1 = value1.getOldcount();

        long pv2 = value1.getPv();
        long uv2 = value1.getUv();
        long newcountvalue2 = value2.getNewcount();
        long oldcountvalue2 = value2.getOldcount();

        PindaoArea pindaoArea = new PindaoArea();
        pindaoArea.setPindaoid(pingdaoid);
        pindaoArea.setArea(area);
        pindaoArea.setTimestamp(timestampvalue);
        pindaoArea.setTimestring(timestring);
        pindaoArea.setNewcount(newcountvalue1+newcountvalue2);
        pindaoArea.setOldcount(oldcountvalue1+oldcountvalue2);
        pindaoArea.setPv(pv1+pv2);
        pindaoArea.setUv(uv1+uv2);
        System.out.println( "recuduce --pidaoArea=="+pindaoArea);
        return  pindaoArea;
    }
}


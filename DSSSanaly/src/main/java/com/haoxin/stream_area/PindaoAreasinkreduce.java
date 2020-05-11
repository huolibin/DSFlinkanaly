package com.haoxin.stream_area;

import com.alibaba.fastjson.JSONObject;
import com.haoxin.stream_xinxiandu.PindaoXinXianDu;
import com.haoxin.util.HbaseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.scala.map;

import java.util.HashMap;
import java.util.Map;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/7 17:02
 */
public class PindaoAreasinkreduce implements SinkFunction<PindaoArea> {
    @Override
    public void invoke(PindaoArea value, Context context) throws Exception {
        System.out.println( "recuducesinkd --pidaoArea=="+value);
        long pingdaoid = value.getPindaoid();
        long newcount = value.getNewcount();
        long oldcount = value.getOldcount();
        String area = value.getArea();
        long pvcount = value.getPv();
        long uvcount = value.getUv();
        String timestring = value.getTimestring();

        String pv = HbaseUtil.getdata("pindaoinfo",pingdaoid+timestring,"info","areapv");
        String uv = HbaseUtil.getdata("pindaoinfo",pingdaoid+timestring,"info","areapv");

        String newcountstr = HbaseUtil.getdata("pindaoinfo",pingdaoid+timestring,"info","areanewcount");
        String oldcountstr = HbaseUtil.getdata("pindaoinfo",pingdaoid+timestring,"info","areaoldcount");


        Map<String, String> datamap = new HashMap<>();

        /**
         * pv
         */
        Map<String, Long> map = new HashMap<>();//存放pv
        if (StringUtils.isNoneBlank(pv)){
            map = JSONObject.parseObject(pv, Map.class);
            Long aLong = map.get(area);
            if (aLong != null){
                pvcount += aLong;
            }
        }
        map.put(area,pvcount);
        datamap.put("areapv",JSONObject.toJSONString(map));

        /**
         * uv
         */
        map = new HashMap<>();//存放uv
        if (StringUtils.isNoneBlank(uv)){
            map = JSONObject.parseObject(uv, Map.class);
            Long aLong = map.get(area);
            if (aLong != null){
                uvcount += aLong;
            }
        }
        map.put(area,uvcount);
        datamap.put("areauv",JSONObject.toJSONString(map));

        /**
         * newcount
         */
        map = new HashMap<>();//存放new
        if(StringUtils.isNotBlank(newcountstr)){
           map =JSONObject.parseObject(newcountstr,Map.class);
            Long count = map.get(area);
            if (count != null){
                newcount += newcount + count;
            }
        }
        map.put(area,newcount);
        datamap.put("areanewcount",JSONObject.toJSONString(map));

        /**
         * oldcount
         */
        map = new HashMap<>();//存放old
        if(StringUtils.isNotBlank(oldcountstr)){
            map =JSONObject.parseObject(oldcountstr,Map.class);
            Long count1 = map.get(area);
            if (count1 != null){
                oldcount += oldcount + count1;
            }
        }
        map.put(area,oldcount);
        datamap.put("areaoldcount",JSONObject.toJSONString(map));


        System.out.println( "pidaopvuv---- HbaseUtil.put(pindaoinfo+"+","+pingdaoid+timestring+",info"+datamap+")");
        HbaseUtil.put("pindaoinfo",pingdaoid+timestring,"info",datamap);

    }
}


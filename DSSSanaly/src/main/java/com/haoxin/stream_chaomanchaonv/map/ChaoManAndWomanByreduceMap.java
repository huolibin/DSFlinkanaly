package com.haoxin.stream_chaomanchaonv.map;

import com.alibaba.fastjson.JSONObject;
import com.haoxin.stream_chaomanchaonv.util.ChaoManAndWomanInfo;
import com.haoxin.util.HbaseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/3 17:25
 */
public class ChaoManAndWomanByreduceMap implements FlatMapFunction<ChaoManAndWomanInfo, ChaoManAndWomanInfo> {
    @Override
    public void flatMap(ChaoManAndWomanInfo chaoManAndWomanInfo, Collector<ChaoManAndWomanInfo> collector) throws Exception {
        Map<String, Long> resultmap = new HashMap<>();//存储chaotype,并初始化
        resultmap.put("1", 0l);
        resultmap.put("2", 0l);
        String userid = chaoManAndWomanInfo.getUserid();
        List<ChaoManAndWomanInfo> list = chaoManAndWomanInfo.getList();
        for (ChaoManAndWomanInfo chaopai : list) {
            String chaotype = chaopai.getChaotype();
            long count = chaopai.getCount();
            resultmap.put(chaotype, resultmap.get(chaotype) + count);
        }
        //从Hbase中获取数据
        String tablename = "userflaginfo";
        String rowkey = userid;
        String famliyname = "userbehavior";
        String colum = "chaomanandwoman";
        String getdata = HbaseUtil.getdata(tablename, rowkey, famliyname, colum);
        if (StringUtils.isNoneBlank(getdata)) {
            //数据存在就写入resultmap
            Map<String, Long> datamap = JSONObject.parseObject(getdata, Map.class);
            Set<String> keySet = resultmap.keySet();
            for (String key : keySet) {
                long pre = datamap.get(key) == null ? 0l : datamap.get(key);
                resultmap.put(key, resultmap.get(key) + pre);
            }
        }


        if (!resultmap.isEmpty()) {
            String chaomananwomanmapstr = JSONObject.toJSONString(resultmap);
            HbaseUtil.putdata(tablename, rowkey, famliyname, colum, chaomananwomanmapstr);//resultmap数据不为空，就再次写入到Hbase

            long chaoman = resultmap.get("1");//man-count
            long chaowoman = resultmap.get("2");//woman-count

            String flag = "woman";//默认是女
            long finalcount = chaowoman;

            if (chaoman > chaowoman) {
                flag = "man";
                finalcount = chaoman;
            }
            //浏览量超过2000的才是潮男or潮女
            if (finalcount > 2000) {
                colum = "chaotype";
                ChaoManAndWomanInfo chaoManAndWomanInfo1 = new ChaoManAndWomanInfo();
                chaoManAndWomanInfo1.setUserid(rowkey);
                chaoManAndWomanInfo1.setChaotype(flag);
                chaoManAndWomanInfo1.setCount(1l);
                chaoManAndWomanInfo1.setGroupbyfield(flag + "==chaomanandwomaninfo");
                String type = HbaseUtil.getdata(tablename, rowkey, famliyname, colum);
                if (StringUtils.isNoneBlank(type) && !type.equals(flag)) {
                    ChaoManAndWomanInfo chaoManAndWomanInfo2 = new ChaoManAndWomanInfo();
                    chaoManAndWomanInfo2.setUserid(rowkey);
                    chaoManAndWomanInfo2.setChaotype(type);
                    chaoManAndWomanInfo2.setCount(-1l);
                    chaoManAndWomanInfo2.setGroupbyfield(flag + "==chaomanandwomaninfo");
                    collector.collect(chaoManAndWomanInfo2);
                }
                HbaseUtil.putdata(tablename, rowkey, famliyname, colum, flag);//写入hbase 是否是chaoman orchaowoman
                collector.collect(chaoManAndWomanInfo1);
            }

        }


    }
}

package com.haoxin.stream_chaomanchaonv.map;

import com.alibaba.fastjson.JSONObject;
import com.haoxin.stream_brandlike.util.KafkaEvent;
import com.haoxin.stream_brandlike.util.ScanProductLog;
import com.haoxin.stream_chaomanchaonv.util.ChaoManAndWomanInfo;
import com.haoxin.util.ReadProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on  2020/6/3 16:39
 * 对kafka中的KafkaEvent转变成ChaoManAndWomanInfo数据类型
 */
public class ChaoManAndWomanMap implements FlatMapFunction<KafkaEvent, ChaoManAndWomanInfo> {

    @Override
    public void flatMap(KafkaEvent kafkaEvent, Collector<ChaoManAndWomanInfo> collector) throws Exception {
        String data = kafkaEvent.getWord();
        ScanProductLog scanProductLog = JSONObject.parseObject(data, ScanProductLog.class);
        int userid = scanProductLog.getUserid();
        int productid = scanProductLog.getProductid();
        String chaotype = ReadProperties.getKey(productid + "", "productChaoLiudic.properties");//通过properties查找到是否是潮牌
        ChaoManAndWomanInfo chaoManAndWomanInfo = new ChaoManAndWomanInfo();
        chaoManAndWomanInfo.setUserid(userid + "");
        if (StringUtils.isNoneBlank(chaotype)) {
            chaoManAndWomanInfo.setChaotype(chaotype);
            chaoManAndWomanInfo.setCount(1l);
            chaoManAndWomanInfo.setGroupbyfield("chaomanandwomen==" + userid);
            ArrayList<ChaoManAndWomanInfo> list = new ArrayList<>();
            list.add(chaoManAndWomanInfo);
            chaoManAndWomanInfo.setList(list);
            collector.collect(chaoManAndWomanInfo);
        }

    }
}

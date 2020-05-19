package com.haoxin.stream_matrix.map;

import com.haoxin.stream_matrix.util.LogicInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Random;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/19 15:16
 */
public class LogicMap implements MapFunction<String, LogicInfo> {
    @Override
    public LogicInfo map(String s) throws Exception {

        if (StringUtils.isBlank(s)){
            return null;
        }
        Random random = new Random();
        String[] split = s.split(",");
        String variable1 = split[0];
        String variable2 = split[1];
        String variable3 = split[2];
        String labase = split[3];

        LogicInfo logicInfo = new LogicInfo();
        logicInfo.setVariable1(variable1);
        logicInfo.setVariable2(variable2);
        logicInfo.setVariable3(variable3);
        logicInfo.setLabase(labase);
        logicInfo.setGroupbyfield("logic=="+random.nextInt(10));
        return logicInfo;
    }
}

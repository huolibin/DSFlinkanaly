package com.haoxin.stream_brandlike.util;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/18 12:28
 */
public class MapUtils {

    public static String getmaxbrand(Map<String,Long> map){
        TreeMap<Long, String> treeMap = new TreeMap<>(new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return o2.compareTo(o1);
            }
        });
        Set<Map.Entry<String, Long>> entries = map.entrySet();
        for (Map.Entry<String, Long> set:entries){
            String key = set.getKey();
            Long value = set.getValue();

            treeMap.put(value,key);
        }
        Map.Entry<Long, String> longStringEntry = treeMap.firstEntry();
        return longStringEntry.getValue();
    }

}

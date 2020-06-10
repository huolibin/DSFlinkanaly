package com.haoxin.batch_tfidf_searchkey.map;

import com.haoxin.batch_tfidf_searchkey.util.SearchKeyTfIdfEntity;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/8 18:11
 */

/**
 * 数据格式是: userid, 小米 全面游戏手机 大米  娱乐
 */
public class SearchKeyMap implements MapFunction<String, SearchKeyTfIdfEntity> {
    @Override
    public SearchKeyTfIdfEntity map(String s) throws Exception {
        String[] split = s.split(",");
        String userid = split[0];
        String wordarray = split[1];

        SearchKeyTfIdfEntity searchKeyTfIdfEntity = new SearchKeyTfIdfEntity();
        searchKeyTfIdfEntity.setUserid(userid);
        ArrayList<String> list = new ArrayList<>();
        list.add(wordarray);
        searchKeyTfIdfEntity.setOriginalwords(list);
        return searchKeyTfIdfEntity;
    }
}

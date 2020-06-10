package com.haoxin.batch_tfidf_searchkey.reduce;

import com.haoxin.batch_tfidf_searchkey.util.SearchKeyTfIdfEntity;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.List;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/8 18:11
 */
public class SearchKeyReduce implements ReduceFunction<SearchKeyTfIdfEntity> {
    @Override
    public SearchKeyTfIdfEntity reduce(SearchKeyTfIdfEntity value1, SearchKeyTfIdfEntity value2) throws Exception {
        String userid = value1.getUserid();
        List<String> word1 = value1.getOriginalwords();
        List<String> word2 = value2.getOriginalwords();
        word1.addAll(word2);
        SearchKeyTfIdfEntity searchKeyTfIdfEntity = new SearchKeyTfIdfEntity();
        searchKeyTfIdfEntity.setUserid(userid);
        searchKeyTfIdfEntity.setOriginalwords(word1);
        return searchKeyTfIdfEntity;
    }
}

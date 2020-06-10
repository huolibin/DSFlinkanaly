package com.haoxin.batch_tfidf_searchkey.reduce;

import com.haoxin.batch_tfidf_searchkey.util.SearchKeyTfIdfEntity;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.List;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/8 18:11
 */
public class SearchKeyReduce2 implements ReduceFunction<SearchKeyTfIdfEntity> {
    @Override
    public SearchKeyTfIdfEntity reduce(SearchKeyTfIdfEntity value1, SearchKeyTfIdfEntity value2) throws Exception {
        Long count1 = value1.getTotaldocumet();
        Long count2 = value1.getTotaldocumet();
        SearchKeyTfIdfEntity searchKeyTfIdfEntity = new SearchKeyTfIdfEntity();
        searchKeyTfIdfEntity.setTotaldocumet(count1+count2);
        return searchKeyTfIdfEntity;
    }
}

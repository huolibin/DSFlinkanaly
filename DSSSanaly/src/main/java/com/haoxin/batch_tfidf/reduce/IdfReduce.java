package com.haoxin.batch_tfidf.reduce;

import com.haoxin.batch_tfidf.util.TfIdfEntity;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/8 10:12
 *
 * 对单词个数的累加
 */
public class IdfReduce implements ReduceFunction<TfIdfEntity> {
    @Override
    public TfIdfEntity reduce(TfIdfEntity t1, TfIdfEntity t2) throws Exception {
        Long count1 = t1.getTotaldocumet();
        Long count2 = t2.getTotaldocumet();
        TfIdfEntity tfIdfEntity = new TfIdfEntity();
        tfIdfEntity.setTotaldocumet(count1+count2);

        return tfIdfEntity;
    }
}

package com.haoxin.stream_chaomanchaonv.reduce;

import com.haoxin.stream_chaomanchaonv.util.ChaoManAndWomanInfo;
import com.haoxin.util.MongoUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/3 18:25
 */
public class ChaoManAndWomanSink implements SinkFunction<ChaoManAndWomanInfo> {

    @Override
    public void invoke(ChaoManAndWomanInfo value, Context context) throws Exception {
        String chaotype = value.getChaotype();
        long count = value.getCount();
        Document doc = MongoUtil.findoneby("chaomananwomanstatics", "haoxinprotrain", chaotype);
        if (doc == null) {
            doc = new Document();
            doc.put("info", chaotype);
            doc.put("count", count);
        } else {
            long pre = doc.getLong("count");
            long total = pre + count;
            doc.put("count", total);
        }
        MongoUtil.saveorupdatemongo("chaomananwomanstatics", "haoxinprotrain", doc);
    }
}

package com.haoxin.batch_tfidf_searchkey.map;

import com.haoxin.batch_tfidf.util.TfIdfEntity;
import com.haoxin.batch_tfidf_searchkey.util.SearchKeyTfIdfEntity;
import com.haoxin.util.HbaseUtil;
import com.haoxin.util.IKUtil;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.*;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/9 14:44
 *
 * 并把数据保存在Hbase中
 */
public class SearchkeyMap2 implements MapFunction<SearchKeyTfIdfEntity, SearchKeyTfIdfEntity> {
    @Override
    public SearchKeyTfIdfEntity map(SearchKeyTfIdfEntity s) throws Exception {
        String userid = s.getUserid();
        List<String> words = s.getOriginalwords();
        Map<String, Long> tfmap = new HashMap<>();//tfmap
        Set<String> wordset = new HashSet<>();//word set
        for (String str:words
             ) {
            List<String> ikWord = IKUtil.getIkWord(str);
            for (String word:ikWord) {
                wordset.add(word);
                Long pre = tfmap.get(word)==null?0l:tfmap.get(word);
                tfmap.put(word,pre+1);
            }
        }

        //总分词个数
        Collection<Long> values = tfmap.values();
        long sumtf = 0l;
        for (Long templog:values){
            sumtf+=templog;
        }

        Map<String,Double> tfmapfinal = new HashMap<>();//存储tf
        Set<Map.Entry<String, Long>> tfentry = tfmap.entrySet();
        for (Map.Entry<String, Long> entrytf:tfentry) {
            String word = entrytf.getKey();
            Long count = entrytf.getValue();
            double tf = Double.valueOf(count) / Double.valueOf(sumtf);
            tfmapfinal.put(word, tf);
        }

        SearchKeyTfIdfEntity searchKeyTfIdfEntity = new SearchKeyTfIdfEntity();
        searchKeyTfIdfEntity.setUserid(userid);
        searchKeyTfIdfEntity.setDatamap(tfmap);
        searchKeyTfIdfEntity.setTfmap(tfmapfinal);
        searchKeyTfIdfEntity.setTotaldocumet(1l);

        //create "tfidfdata,"baseinfo"
        for(String word:wordset){
            String tablename = "keyworddata";
            String rowkey=word;
            String famliyname="baseinfo";
            String colum="idfcount";
            String data = HbaseUtil.getdata(tablename,rowkey,famliyname,colum);
            Long pre = data==null?0l:Long.valueOf(data);
            Long total = pre+1;
            HbaseUtil.putdata(tablename,rowkey,famliyname,colum,total+"");
        }
        return searchKeyTfIdfEntity;

    }
}

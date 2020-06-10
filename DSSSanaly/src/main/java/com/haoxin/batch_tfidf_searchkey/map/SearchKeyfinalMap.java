package com.haoxin.batch_tfidf_searchkey.map;

import com.haoxin.batch_tfidf.util.TfIdfEntity;
import com.haoxin.batch_tfidf_searchkey.util.SearchKeyTfIdfEntity;
import com.haoxin.util.HbaseUtil;
import com.haoxin.util.MapUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.*;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/8 11:52
 */
public class SearchKeyfinalMap implements MapFunction<SearchKeyTfIdfEntity,SearchKeyTfIdfEntity> {

    private long totaldocuments = 0l;
    private long words;
    private String columnName;
    public SearchKeyfinalMap(long totaldocuments, long words,String columnName){
        this.totaldocuments =totaldocuments;
        this.words = words;
        this.columnName =columnName;
    }

    @Override
    public SearchKeyTfIdfEntity map(SearchKeyTfIdfEntity tfIdfEntity) throws Exception {
        String userid = tfIdfEntity.getUserid();
        Map<String, Double> tfmap = tfIdfEntity.getTfmap();
        Set<Map.Entry<String, Double>> set = tfmap.entrySet();
        String tablename = "keyworddata";
        String famliyname="baseinfo";
        String colum="idfcount";
        Map<String, Double> tfidfmap= new HashMap<String,Double>();
        for (Map.Entry<String, Double> entry:set) {
            String word = entry.getKey();
            Double value = entry.getValue();
            String getdata = HbaseUtil.getdata(tablename, word, famliyname, colum);
            Long viewcount = Long.valueOf(getdata);
            double idf = Math.log(totaldocuments/(viewcount + 1));
            double tfidf = value*idf;
            tfidfmap.put(word,tfidf);
        }

        LinkedHashMap<String, Double> resultfinal = MapUtil.sortMapByValue(tfidfmap);
        Set<Map.Entry<String, Double>> entrySet = resultfinal.entrySet();
        List<String> finalword = new ArrayList<>();
        int count =1;
        for (Map.Entry<String, Double> mapset:entrySet) {
            finalword.add(mapset.getKey());
            count++;
            if (count > words){
                break;
            }
        }
        SearchKeyTfIdfEntity searchKeyTfIdfEntity = new SearchKeyTfIdfEntity();
        searchKeyTfIdfEntity.setUserid(userid);
        searchKeyTfIdfEntity.setFinalword(finalword);

        //将最后的结果保存在Hbase
        String keywordstring ="";
        for (String s:finalword) {
            keywordstring +=s+",";
        }
        if(StringUtils.isNoneBlank(keywordstring)){
            String tablename1 = "userkeyworddata";
            String rowkey1 = userid;
            String famliyname1="baseinfo";
            String colum1=columnName;
            HbaseUtil.putdata(tablename1,rowkey1,famliyname1,colum1,keywordstring);
        }

        return searchKeyTfIdfEntity;
    }
}

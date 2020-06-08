package com.haoxin.batch_tfidf.map;

import com.haoxin.batch_tfidf.util.TfIdfEntity;
import com.haoxin.util.HbaseUtil;
import com.haoxin.util.MapUtil;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.*;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/8 11:52
 */
public class IdffinalMap implements MapFunction<TfIdfEntity,TfIdfEntity> {

    private long totaldocuments = 0l;
    private long words;
    public IdffinalMap(long totaldocuments,long words){
        this.totaldocuments =totaldocuments;
        this.words = words;
    }

    @Override
    public TfIdfEntity map(TfIdfEntity tfIdfEntity) throws Exception {
        String documentid = tfIdfEntity.getDocumentid();
        Map<String, Double> tfmap = tfIdfEntity.getTfmap();
        Set<Map.Entry<String, Double>> set = tfmap.entrySet();
        String tablename = "tfidfdata";
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
        TfIdfEntity tfIdfEntity1 = new TfIdfEntity();
        tfIdfEntity1.setDocumentid(documentid);
        tfIdfEntity1.setFinalword(finalword);
        return tfIdfEntity1;
    }
}

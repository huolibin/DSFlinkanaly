package com.haoxin.batch_tfidf.map;

import com.haoxin.batch_tfidf.util.TfIdfEntity;
import com.haoxin.util.HbaseUtil;
import com.haoxin.util.IKUtil;
import com.haoxin.util.MapUtil;
import org.apache.flink.api.common.functions.MapFunction;

import java.io.BufferedReader;
import java.util.*;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/5 17:44
 * 对String数据进行处理，
 * 为每一行数据切割、计算，成TfIdfEntity格式数据
 * 并把数据保存在Hbase中
 */
public class IdfMap implements MapFunction<String, TfIdfEntity> {
    @Override
    public TfIdfEntity map(String s) throws Exception {
        Map<String, Long> tfmap = new HashMap<>();//tfmap
        Set<String> wordset = new HashSet<>();//word set

        List<String> ikWord = IKUtil.getIkWord(s);
        String docid = UUID.randomUUID().toString();//行id
        for (String word:ikWord) {
            wordset.add(word);
            Long pre = tfmap.get(word)==null?0l:tfmap.get(word);
            tfmap.put(word,pre+1);
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

        TfIdfEntity tfIdfEntity = new TfIdfEntity();
        tfIdfEntity.setDocumentid(docid);
        tfIdfEntity.setDatamap(tfmap);
        tfIdfEntity.setTfmap(tfmapfinal);
        tfIdfEntity.setTotaldocumet(1l);

        //create "tfidfdata,"baseinfo"
        for(String word:wordset){
            String tablename = "tfidfdata";
            String rowkey=word;
            String famliyname="baseinfo";
            String colum="idfcount";
            String data = HbaseUtil.getdata(tablename,rowkey,famliyname,colum);
            Long pre = data==null?0l:Long.valueOf(data);
            Long total = pre+1;
            HbaseUtil.putdata(tablename,rowkey,famliyname,colum,total+"");
        }
        return tfIdfEntity;

    }
}

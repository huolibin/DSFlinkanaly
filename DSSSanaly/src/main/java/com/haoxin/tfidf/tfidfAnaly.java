package com.haoxin.tfidf;

import com.haoxin.util.IKUtil;
import com.haoxin.util.MapUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/5 14:04
 */
public class tfidfAnaly {
    public static void main(String[] args) throws Exception{
        Map<String, Long> tfmap = new HashMap<>();//tfmap
        String filepath ="E:\\ceshi\\iktest\\test1.txt";
        File file = new File(filepath);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        String temp="";
        Set<String> wordset = new HashSet<>();//word set
        ArrayList<String> datalist = new ArrayList<>();//源数据，一行一行的存储
        HashMap<String, Map<String, Long>> documenttfmap = new HashMap<>();//每行数据，的汇总
        while((temp = bufferedReader.readLine()) != null ){
            List<String> ikWord = IKUtil.getIkWord(temp);
            String docid = UUID.randomUUID().toString();//行id
            for (String word:ikWord) {
                wordset.add(word);
                tfmap = documenttfmap.get(docid)==null?new HashMap<String, Long>():documenttfmap.get(docid);
                Long pre = tfmap.get(word)==null?0l:tfmap.get(word);
                tfmap.put(word,pre+1);
                documenttfmap.put(docid,tfmap);


            }

        }

        Map<String, Long> idfmap =  new HashMap<String,Long>();//idfmap
        for (String set:wordset) {
            for (String tempdata:datalist) {
                if (IKUtil.getIkWord(tempdata).contains(set)){
                    Long pre = idfmap.get(set)==null?0l:idfmap.get(set);
                    idfmap.put(set,pre+1);
                }
            }
        }

        int alldocumynums = documenttfmap.keySet().size();//tf行数
        Set<Map.Entry<String, Map<String, Long>>> docset = documenttfmap.entrySet();
        for (Map.Entry<String, Map<String, Long>> entry:docset) {
            String documentid = entry.getKey();
            Map<String,Double> tfidmap = new HashMap<>();
            Map<String, Long> tfmaptemp = entry.getValue();
            Collection<Long> values = tfmaptemp.values();
            long sumtf = 0l;//每行的总分词数
            for (Long templog:values){
                sumtf+=templog;
            }

            Set<Map.Entry<String, Long>> tfentry = tfmaptemp.entrySet();
            for (Map.Entry<String, Long> entrytf:tfentry) {
                String word = entrytf.getKey();
                Long count = entrytf.getValue();
                double tf = Double.valueOf(count)/Double.valueOf(sumtf);
                double idf = Math.log(Double.valueOf(alldocumynums)/Double.valueOf(idfmap.get(word)));

                double tfidf =tf*idf;
                tfidmap.put(word,tfidf);
            }
            LinkedHashMap<String, Double> sortedMap = MapUtil.sortMapByValue(tfidmap);
            Set<Map.Entry<String, Double>> setfinal = sortedMap.entrySet();
            int count=1;
            for (Map.Entry<String, Double> entry1:setfinal) {
                if (count>3){
                    break;
                }
                System.out.println(entry1.getKey());
                count++;
            }

        }
    }
}

package com.haoxin.batch_tfidf_searchkey.util;

import java.util.List;
import java.util.Map;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/5 17:41
 */
public class SearchKeyTfIdfEntity {
    private String userid;//用户id
    private Map<String,Long> datamap;//tfmap
    private Map<String,Double> tfmap;//tf
    private Long totaldocumet;//总id数
    private List<String> finalword;
    private List<String> originalwords;//搜素语句

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public List<String> getOriginalwords() {
        return originalwords;
    }

    public void setOriginalwords(List<String> originalwords) {
        this.originalwords = originalwords;
    }

    public Map<String, Long> getDatamap() {
        return datamap;
    }

    public void setDatamap(Map<String, Long> datamap) {
        this.datamap = datamap;
    }

    public Map<String, Double> getTfmap() {
        return tfmap;
    }

    public void setTfmap(Map<String, Double> tfmap) {
        this.tfmap = tfmap;
    }

    public Long getTotaldocumet() {
        return totaldocumet;
    }

    public void setTotaldocumet(Long totaldocumet) {
        this.totaldocumet = totaldocumet;
    }

    public List<String> getFinalword() {
        return finalword;
    }

    public void setFinalword(List<String> finalword) {
        this.finalword = finalword;
    }
}

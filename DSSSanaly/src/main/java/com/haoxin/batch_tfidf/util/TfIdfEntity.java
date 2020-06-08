package com.haoxin.batch_tfidf.util;

import java.util.List;
import java.util.Map;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/5 17:41
 */
public class TfIdfEntity {
    private String documentid;//id
    private Map<String,Long> datamap;//tfmap
    private Map<String,Double> tfmap;//tf
    private Long totaldocumet;//总id数
    private List<String> finalword;

    public String getDocumentid() {
        return documentid;
    }

    public void setDocumentid(String documentid) {
        this.documentid = documentid;
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

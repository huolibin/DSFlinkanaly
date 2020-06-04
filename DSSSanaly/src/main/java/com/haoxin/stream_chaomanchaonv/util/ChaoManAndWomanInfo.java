package com.haoxin.stream_chaomanchaonv.util;

import java.util.List;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/3 16:31
 */
public class ChaoManAndWomanInfo {
    private String userid;//用户id
    private String chaotype;//类型：1，潮男；2，潮女
    private  long count;
    private  String groupbyfield;

    private List<ChaoManAndWomanInfo> list;

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getChaotype() {
        return chaotype;
    }

    public void setChaotype(String chaotype) {
        this.chaotype = chaotype;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getGroupbyfield() {
        return groupbyfield;
    }

    public void setGroupbyfield(String groupbyfield) {
        this.groupbyfield = groupbyfield;
    }

    public List<ChaoManAndWomanInfo> getList() {
        return list;
    }

    public void setList(List<ChaoManAndWomanInfo> list) {
        this.list = list;
    }
}

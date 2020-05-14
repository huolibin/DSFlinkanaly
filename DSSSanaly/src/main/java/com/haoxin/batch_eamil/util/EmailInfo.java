package com.haoxin.batch_eamil.util;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/14 11:04
 */
public class EmailInfo {
    private String emailtype; //邮件类型
    private long  count;//邮件数量
    private String groupfield;//分组字段

    public String getEmailtype() {
        return emailtype;
    }

    public void setEmailtype(String emailtype) {
        this.emailtype = emailtype;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getGroupfield() {
        return groupfield;
    }

    public void setGroupfield(String groupfield) {
        this.groupfield = groupfield;
    }

    @Override
    public String toString() {
        return "EamilInfo{" +
                "emailtype='" + emailtype + '\'' +
                ", count=" + count +
                ", groupfield='" + groupfield + '\'' +
                '}';
    }
}

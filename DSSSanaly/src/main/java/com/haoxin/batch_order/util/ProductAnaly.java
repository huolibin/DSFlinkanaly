package com.haoxin.batch_order.util;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/12 11:38
 * 产品分析类
 */
public class ProductAnaly {
    private long productid;
    private String dateString;
    private  long chengjiaocount;//成交
    private  long nochjcount;//未成交
    private String groupbyfield;

    public long getProductid() {
        return productid;
    }

    public void setProductid(long productid) {
        this.productid = productid;
    }

    public String getDateString() {
        return dateString;
    }

    public void setDateString(String dateString) {
        this.dateString = dateString;
    }

    public long getChengjiaocount() {
        return chengjiaocount;
    }

    public void setChengjiaocount(long chengjiaocount) {
        this.chengjiaocount = chengjiaocount;
    }

    public long getNochjcount() {
        return nochjcount;
    }

    public void setNochjcount(long nochjcount) {
        this.nochjcount = nochjcount;
    }

    public String getGroupbyfield() {
        return groupbyfield;
    }

    public void setGroupbyfield(String groupbyfield) {
        this.groupbyfield = groupbyfield;
    }

    @Override
    public String toString() {
        return "ProductAnaly{" +
                "productid=" + productid +
                ", dateString='" + dateString + '\'' +
                ", chengjiaocount=" + chengjiaocount +
                ", nochjcount=" + nochjcount +
                ", groupbyfield='" + groupbyfield + '\'' +
                '}';
    }
}

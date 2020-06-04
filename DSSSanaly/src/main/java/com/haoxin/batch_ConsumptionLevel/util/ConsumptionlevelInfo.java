package com.haoxin.batch_ConsumptionLevel.util;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/14 16:53
 * <p>
 * 对消费水平的类别
 */
public class ConsumptionlevelInfo {
    private String userid;//用户id
    private double totalamount;//消费金额
    private String consumptionlevel;//消费水平（高水平 中等水平 低水平）
    private Long count;//数量
    private String groupField;//分组

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public double getTotalamount() {
        return totalamount;
    }

    public void setTotalamount(double totalamount) {
        this.totalamount = totalamount;
    }

    public String getConsumptionlevel() {
        return consumptionlevel;
    }

    public void setConsumptionlevel(String consumptionlevel) {
        this.consumptionlevel = consumptionlevel;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getGroupField() {
        return groupField;
    }

    public void setGroupField(String groupField) {
        this.groupField = groupField;
    }
}

package com.haoxin.stream_xinxiandu;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/9 16:10
 * 频道新鲜度类方法
 */
public class PindaoXinXianDu {
    private long pindaoid;
    private long newcount;
    private long oldcount;
    private long timestamp;
    private String timestring;
    private String groupbyfield;

    public long getPindaoid() {
        return pindaoid;
    }

    public void setPindaoid(long pindaoid) {
        this.pindaoid = pindaoid;
    }

    public long getNewcount() {
        return newcount;
    }

    public void setNewcount(long newcount) {
        this.newcount = newcount;
    }

    public long getOldcount() {
        return oldcount;
    }

    public void setOldcount(long oldcount) {
        this.oldcount = oldcount;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getTimestring() {
        return timestring;
    }

    public void setTimestring(String timestring) {
        this.timestring = timestring;
    }

    public String getGroupbyfield() {
        return groupbyfield;
    }

    public void setGroupbyfield(String groupbyfield) {
        this.groupbyfield = groupbyfield;
    }

    public PindaoXinXianDu() {
    }

    public PindaoXinXianDu(long pindaoid, long newcount, long oldcount, long timestamp, String timestring, String groupbyfield) {
        this.pindaoid = pindaoid;
        this.newcount = newcount;
        this.oldcount = oldcount;
        this.timestamp = timestamp;
        this.timestring = timestring;
        this.groupbyfield = groupbyfield;
    }

    @Override
    public String toString() {
        return "PindaoXinXianDu{" +
                "pindaoid=" + pindaoid +
                ", newcount=" + newcount +
                ", oldcount=" + oldcount +
                ", timestamp=" + timestamp +
                ", timestring='" + timestring + '\'' +
                ", groupbyfield='" + groupbyfield + '\'' +
                '}';
    }
}

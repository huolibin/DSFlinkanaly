package com.haoxin.stream_area;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/9 17:27
 */
public class PindaoArea {
    private long pindaoid;
    private String area;//地区
    private long pv;
    private long uv;
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

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public long getPv() {
        return pv;
    }

    public void setPv(long pv) {
        this.pv = pv;
    }

    public long getUv() {
        return uv;
    }

    public void setUv(long uv) {
        this.uv = uv;
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

    public void setGroupbyfield(String groupbufield) {
        this.groupbyfield = groupbufield;
    }

    public PindaoArea(long pindaoid, String area, long pv, long uv, long newcount, long oldcount, long timestamp, String timestring, String groupbufield) {
        this.pindaoid = pindaoid;
        this.area = area;
        this.pv = pv;
        this.uv = uv;
        this.newcount = newcount;
        this.oldcount = oldcount;
        this.timestamp = timestamp;
        this.timestring = timestring;
        this.groupbyfield = groupbufield;
    }

    public PindaoArea() {
    }

    @Override
    public String toString() {
        return "PindaoArea{" +
                "pindaoid=" + pindaoid +
                ", area='" + area + '\'' +
                ", pv=" + pv +
                ", uv=" + uv +
                ", newcount=" + newcount +
                ", oldcount=" + oldcount +
                ", timestamp=" + timestamp +
                ", timestring='" + timestring + '\'' +
                ", groupbyfield='" + groupbyfield + '\'' +
                '}';
    }
}

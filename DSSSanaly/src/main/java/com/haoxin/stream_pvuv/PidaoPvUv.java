package com.haoxin.stream_pvuv;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/7 16:41
 */
public class PidaoPvUv {
    private long pingdaoid;
    private long userid;
    private long pvcount;
    private long uvcount;
    private long timestamp;
    private String timestring;
    private String groupbyfield;

    public long getPingdaoid() {
        return pingdaoid;
    }

    public void setPingdaoid(long pingdaoid) {
        this.pingdaoid = pingdaoid;
    }

    public long getUserid() {
        return userid;
    }

    public void setUserid(long userid) {
        this.userid = userid;
    }

    public long getPvcount() {
        return pvcount;
    }

    public void setPvcount(long pvcount) {
        this.pvcount = pvcount;
    }

    public long getUvcount() {
        return uvcount;
    }

    public void setUvcount(long uvcount) {
        this.uvcount = uvcount;
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

    @Override
    public String toString() {
        return "PidaoPvUv{" +
                "pingdaoid=" + pingdaoid +
                ", userid=" + userid +
                ", pvcount=" + pvcount +
                ", uvcount=" + uvcount +
                ", timestamp=" + timestamp +
                ", timestring='" + timestring + '\'' +
                ", groupbyfield='" + groupbyfield + '\'' +
                '}';
    }
}

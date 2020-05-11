package com.haoxin.stream_browser;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/11 15:14
 */
public class UserBrowser {
    private String browser;
    private long count;
    private long newcount;
    private long oldcount;
    private long timestamp;
    private String timestring;

    public String getBrowser() {
        return browser;
    }

    public void setBrowser(String browser) {
        this.browser = browser;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
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

    @Override
    public String toString() {
        return "UserBrowser{" +
                "browser='" + browser + '\'' +
                ", count=" + count +
                ", newcount=" + newcount +
                ", oldcount=" + oldcount +
                ", timestamp=" + timestamp +
                ", timestring='" + timestring + '\'' +
                '}';
    }
}

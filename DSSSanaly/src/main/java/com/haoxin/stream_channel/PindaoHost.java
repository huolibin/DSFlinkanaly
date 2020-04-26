package com.haoxin.stream_channel;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/4/21 16:23
 */
public class PindaoHost {
    private Long pindaoid;
    private Long count;

    public Long getPindaoid() {
        return pindaoid;
    }

    public void setPindaoid(Long pindaoid) {
        this.pindaoid = pindaoid;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "PindaoHost{" +
                "pindaoid=" + pindaoid +
                ", count=" + count +
                '}';
    }
}

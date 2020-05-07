package com.haoxin.stream_pvuv;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/7 16:42
 */
public class UserState {
    private boolean isnew = false;//是否是新来的用户
    private boolean isFisrthour = false;//是否是小时第一次来
    private boolean isFisrtday = false;//是否是今天第一次来
    private boolean isFisrtmonth = false;//是否是这个月第一次来

    public boolean isnew() {
        return isnew;
    }

    public void setIsnew(boolean isnew) {
        this.isnew = isnew;
    }

    public boolean isFisrthour() {
        return isFisrthour;
    }

    public void setFisrthour(boolean fisrthour) {
        isFisrthour = fisrthour;
    }

    public boolean isFisrtday() {
        return isFisrtday;
    }

    public void setFisrtday(boolean fisrtday) {
        isFisrtday = fisrtday;
    }

    public boolean isFisrtmonth() {
        return isFisrtmonth;
    }

    public void setFisrtmonth(boolean fisrtmonth) {
        isFisrtmonth = fisrtmonth;
    }
}

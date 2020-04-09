package com.haoxin.log;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/4/9 11:39
 * 用户浏览记录
 */
//频道id 类别id 产品id 用户id 打开时间 离开时间 地区 网络方式 来源方式 浏览器
public class Userscanlog {
    private long pindaoids;
    private long leibieids;
    private long chanpinids;
    private long yonghuids;
    private long starttime;
    private long endtime;
    private String contry;
    private String province;
    private String city;
    private String network;
    private String sources;
    private String liulanqitype;

    public Userscanlog() {
    }

    public Userscanlog(long pindaoids, long leibieids, long chanpinids, long yonghuids, long starttime, long endtime, String contry, String province, String city, String network, String sources, String liulanqitype) {

        this.pindaoids = pindaoids;
        this.leibieids = leibieids;
        this.chanpinids = chanpinids;
        this.yonghuids = yonghuids;
        this.starttime = starttime;
        this.endtime = endtime;
        this.contry = contry;
        this.province = province;
        this.city = city;
        this.network = network;
        this.sources = sources;
        this.liulanqitype = liulanqitype;
    }

    public long getPindaoids() {
        return pindaoids;
    }

    public void setPindaoids(long pindaoids) {
        this.pindaoids = pindaoids;
    }

    public long getLeibieids() {
        return leibieids;
    }

    public void setLeibieids(long leibieids) {
        this.leibieids = leibieids;
    }

    public long getChanpinids() {
        return chanpinids;
    }

    public void setChanpinids(long chanpinids) {
        this.chanpinids = chanpinids;
    }

    public long getYonghuids() {
        return yonghuids;
    }

    public void setYonghuids(long yonghuids) {
        this.yonghuids = yonghuids;
    }

    public long getStarttime() {
        return starttime;
    }

    public void setStarttime(long starttime) {
        this.starttime = starttime;
    }

    public long getEndtime() {
        return endtime;
    }

    public void setEndtime(long endtime) {
        this.endtime = endtime;
    }

    public String getContry() {
        return contry;
    }

    public void setContry(String contry) {
        this.contry = contry;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getNetwork() {
        return network;
    }

    public void setNetwork(String network) {
        this.network = network;
    }

    public String getSources() {
        return sources;
    }

    public void setSources(String sources) {
        this.sources = sources;
    }

    public String getLiulanqitype() {
        return liulanqitype;
    }

    public void setLiulanqitype(String liulanqitype) {
        this.liulanqitype = liulanqitype;
    }

}

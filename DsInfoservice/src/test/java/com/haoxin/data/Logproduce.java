package com.haoxin.data;

import com.alibaba.fastjson.JSONObject;
import com.haoxin.log.Userscanlog;
import com.haoxin.utils.UrlSendUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/4/9 10:50
 * 创造一些数据，数据格式如下：
 *            频道id 类别id 产品id 用户id 打开时间 离开时间 地区 网络方式 来源方式 浏览器
 *   Spring 上报产品数据
 */
public class Logproduce {
    private static Long[] pindaoids = new Long[]{1L,2L,3L,4L,5L,6L,7L,8L};//频道id
    private static Long[] leibieids = new Long[]{1L,2L,3L,4L,5L,6L,7L,8L};//类别id
    private static Long[] chanpinids = new Long[]{1L,2L,3L,4L,5L,6L,7L,8L};//产品id
    private static Long[] yonghuids = new Long[]{1L,2L,3L,4L,5L,6L,7L,8L};//用户id

    /**
     * 地区
     */
    private static String[] contrys = new String[]{"America","china"};//地区-国家集合
    private static String[] provinces = new String[]{"America","china"};//地区-省集合
    private static String[] citys = new String[]{"America","china"};//地区-市集合

    /**
     *网络方式
     */
    private static String[] networks = new String[]{"电信","移动","联通"};

    /**
     * 来源方式
     */
    private static String[] sources = new String[]{"直接输入","百度跳转","360搜索跳转","必应跳转"};

    /**
     * 浏览器
     */
    private static String[] liulanqis = new String[]{"火狐","qq浏览器","360浏览器","谷歌浏览器"};

    /**
     * 打开时间  离开时间
     */

    private static List<Long[]> arrayList = new Logproduce().producetimes();
    public List<Long[]> producetimes(){
        ArrayList<Long[]> arrayList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Long[] gettimes = gettimes("2020-04-22 11:34:22:123");
            arrayList.add(gettimes);
        }
        return arrayList;
    }

    private Long[] gettimes(String time){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
        try {
            Date date = dateFormat.parse(time);
            long dateTime = date.getTime();
            Random random = new Random();
            int randommint = random.nextInt(10);
            long starttime = dateTime-randommint*3600*1000;
            long endtime = dateTime+randommint*3600*1000;
            return new Long[]{starttime,endtime};
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return new Long[]{0L,0L};
    }


    public static void main(String[] args) {
        Random random = new Random();
        for (int i = 0; i < 20; i++) {
            Userscanlog userscanLog = new Userscanlog();
            userscanLog.setPindaoids(pindaoids[random.nextInt(pindaoids.length)]);
            userscanLog.setLeibieids(leibieids[random.nextInt(leibieids.length)]);
            userscanLog.setChanpinids(chanpinids[random.nextInt(chanpinids.length)]);
            userscanLog.setYonghuids(yonghuids[random.nextInt(yonghuids.length)]);
            userscanLog.setContry(contrys[random.nextInt(contrys.length)]);
            userscanLog.setProvince(provinces[random.nextInt(provinces.length)]);
            userscanLog.setCity(citys[random.nextInt(citys.length)]);

            userscanLog.setNetwork(networks[random.nextInt(networks.length)]);
            userscanLog.setSources(sources[random.nextInt(sources.length)]);
            userscanLog.setLiulanqitype(liulanqis[random.nextInt(liulanqis.length)]);


            Long[] times = arrayList.get(random.nextInt(arrayList.size()));
            userscanLog.setStarttime(times[0]);
            userscanLog.setEndtime(times[1]);

            String jonstr = JSONObject.toJSONString(userscanLog);
            System.out.println(jonstr);
            UrlSendUtil.sendmessage("http://127.0.0.1:6098/DsInfoSJservice/webInfoSJService",jonstr);

        }
    }


}

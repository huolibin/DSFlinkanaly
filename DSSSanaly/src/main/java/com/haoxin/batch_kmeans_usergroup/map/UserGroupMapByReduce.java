package com.haoxin.batch_kmeans_usergroup.map;

import com.haoxin.batch_baijia.util.DatetimeUtil;
import com.haoxin.batch_kmeans_usergroup.util.UserGroupInfo;
import com.haoxin.util.ReadProperties;
import org.apache.flink.api.common.functions.MapFunction;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/2 17:27
 * <p>
 * 将UserGroupInfo数据格式转换成 添加以下格式
 * private double avramout;//平均消费金额
 * private double maxamout;//消费最大金额
 * private int days;//消费频次
 * private Long buytype1;//消费类目1数量
 * private Long buytype2;//消费类目2数量
 * private Long buytype3;//消费类目3数量
 * private Long buytime1;//消费时间点1数量
 * private Long buytime2;//消费时间点2数量
 * private Long buytime3;//消费时间点3数量
 * private Long buytime4;//消费时间点4数量
 */
public class UserGroupMapByReduce implements MapFunction<UserGroupInfo, UserGroupInfo> {
    /**
     * 消费类目3个，电子（电脑，手机，电视）生活家居（衣服、生活用户，床上用品）生鲜（油，米等等）
     * 消费时间点分4个，上午（7-12），下午（12-7），晚上（7-12），凌晨（0-7）
     */
    @Override
    public UserGroupInfo map(UserGroupInfo userGroupInfo) throws Exception {
        List<UserGroupInfo> list = userGroupInfo.getList();

        //对list排序
        Collections.sort(list, new Comparator<UserGroupInfo>() {
            @Override
            public int compare(UserGroupInfo o1, UserGroupInfo o2) {
                String createtime1 = o1.getCreatetime();
                String createtime2 = o2.getCreatetime();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd hhmmss");
                Date date = new Date();
                Date time1 = date;
                Date time2 = date;
                try {
                    time1 = dateFormat.parse(createtime1);
                    time2 = dateFormat.parse(createtime1);
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                return time1.compareTo(time2);
            }
        });

        double totalamount = 0;//总金额
        double maxamout = Double.MIN_VALUE;//消费最大金额
        Map<Integer, Integer> frequencymap = new HashMap<>();//存储消费的频次
        UserGroupInfo usergroupInfobefore = null; //定义前一个usergroupInfo

        Map<String, Long> productmap = new HashMap<>();//存储消费类目
        productmap.put("1", 0l);
        productmap.put("2", 0l);
        productmap.put("3", 0l);

        Map<Integer, Long> timemap = new HashMap<>();//存储消费时间点
        timemap.put(1, 0l);
        timemap.put(2, 0l);
        timemap.put(3, 0l);
        timemap.put(4, 0l);

        for (UserGroupInfo usergroupInfo : list) {
            double currenttotal = Double.valueOf(usergroupInfo.getTotalamount());
            totalamount += currenttotal;
            if (currenttotal > maxamout) {
                maxamout = currenttotal;
            }

            if (usergroupInfobefore == null) {
                usergroupInfobefore = usergroupInfo;
                continue;
            }

            //计算购买的频率
            String beforetime = usergroupInfobefore.getCreatetime();//前一次的时间
            String currenttime = usergroupInfo.getCreatetime();//这次的时间
            int days = DatetimeUtil.getDaysBetweenbyStartAndend(beforetime, currenttime, "yyyyMMdd hhmmsss");//时间差
            int beforecount = frequencymap.get(days) == null ? 0 : frequencymap.get(days);
            frequencymap.put(days, beforecount + 1);

            //计算商品类别
            //消费类目3个，电子（电脑，手机，电视）生活家居（衣服、生活用户，床上用品）生鲜（油，米等等
            String producttypeid = usergroupInfo.getProducttypeid();//获得商品小类别
            String bitproducttypeid = ReadProperties.getKey(producttypeid, "productypedic.properties");
            Long productcount = productmap.get(bitproducttypeid) == null ? 0l : productmap.get(bitproducttypeid);
            productmap.put(bitproducttypeid, productcount + 1);

            //消费时间点,消费时间点分4个，上午（7-12）1，下午（12-7）2，晚上（7-12）3，凌晨（0-7）4
            String time = usergroupInfo.getCreatetime();
            String hours = DatetimeUtil.gethoursbydate(time);
            Integer hoursint = Integer.valueOf(hours);
            int timetype = -1;
            if (hoursint >= 7 && hoursint < 12) {
                timetype = 1;
            } else if (hoursint >= 12 && hoursint < 19) {
                timetype = 2;
            } else if (hoursint >= 19 && hoursint < 24) {
                timetype = 3;
            } else if (hoursint >= 0 && hoursint < 7) {
                timetype = 4;
            }
            Long timecount = timemap.get(timetype) == null ? 0l : timemap.get(timetype);
            timemap.put(timetype, timecount + 1);
        }

        int listsize = list.size();//订单个数
        double avramount = totalamount / listsize;//平均消费金额
        double finalmaxamount = maxamout;//最大金额

        Integer totaldays = 0;
        Set<Map.Entry<Integer, Integer>> entries = frequencymap.entrySet();
        for (Map.Entry<Integer, Integer> set : entries) {
            Integer days = set.getKey();
            Integer count = set.getValue();
            totaldays += days * count;
        }
        int days = totaldays / listsize;//消费频次

        Random random = new Random();
        UserGroupInfo userGroupInfofinal = new UserGroupInfo();
        userGroupInfofinal.setUserid(userGroupInfo.getUserid());
        userGroupInfofinal.setAvramout(avramount);
        userGroupInfofinal.setMaxamout(maxamout);
        userGroupInfofinal.setDays(days);
        userGroupInfofinal.setBuytype1(productmap.get("1"));
        userGroupInfofinal.setBuytype2(productmap.get("2"));
        userGroupInfofinal.setBuytype3(productmap.get("3"));
        userGroupInfofinal.setBuytime1(timemap.get(1));
        userGroupInfofinal.setBuytime2(timemap.get(2));
        userGroupInfofinal.setBuytime3(timemap.get(3));
        userGroupInfofinal.setBuytime4(timemap.get(4));
        userGroupInfofinal.setGroupfield("usergrouykmean" + random.nextInt(100));
        return userGroupInfofinal;
    }
}

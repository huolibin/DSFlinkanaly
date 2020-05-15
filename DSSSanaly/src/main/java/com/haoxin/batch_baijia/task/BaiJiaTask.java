package com.haoxin.batch_baijia.task;

import com.haoxin.batch_baijia.map.BaiJiaMap;
import com.haoxin.batch_baijia.reduce.BaiJiaReduce;
import com.haoxin.batch_baijia.util.BaiJiaInfo;
import com.haoxin.batch_baijia.util.DatetimeUtil;
import com.haoxin.util.HbaseUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/14 17:20
 * <p>
 * 对最近2年内的订单数据的统计
 * 败家指数 = 支付金额平均值*0.3、最大支付金额*0.3、下单频率*0.4
 */
public class BaiJiaTask {
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);

        //source
        DataSource<String> input = env.readTextFile(parameterTool.get("input"));
        //trans
        DataSet<BaiJiaInfo> map = input.map(new BaiJiaMap());
        DataSet<BaiJiaInfo> reduce = map.groupBy("groupfield").reduce(new BaiJiaReduce());

        //计算
        try {
            List<BaiJiaInfo> collectlist = reduce.collect();
            for (BaiJiaInfo baijiainfo : collectlist
            ) {
                String userid = baijiainfo.getUserid();
                List<BaiJiaInfo> list = baijiainfo.getList();
                Collections.sort(list, new Comparator<BaiJiaInfo>() {
                    @Override
                    public int compare(BaiJiaInfo o1, BaiJiaInfo o2) {

                        String time1 = o1.getCreatetime();
                        String time2 = o2.getCreatetime();
                        Date nowdate = new Date();
                        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd hhmmss");
                        Date parse1 = nowdate;
                        Date parse2 = nowdate;
                        try {
                            parse1 = format.parse(time1);
                            parse2 = format.parse(time2);

                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        return parse1.compareTo(parse2);
                    }
                });//对list按照创建时间 正序排序

                /**
                 * 对每个userid中的list遍历
                 */
                BaiJiaInfo baijiainfopre = null;//前一个baijiainfo
                HashMap<Integer, Integer> frequencymap = new HashMap<>();//存放<天数,个数>
                double maxmount = 0;//最大金额
                double sum = 0; //总金额
                for (BaiJiaInfo baiJiaInfoinner : list) {
                    if (baijiainfopre == null) {
                        baijiainfopre = baiJiaInfoinner;
                        continue;
                    }
                    //计算购买频率
                    String pretime = baijiainfopre.getCreatetime();//前一个订单创建时间
                    String endtime = baiJiaInfoinner.getCreatetime();//当前订单的创建时间
                    int days = DatetimeUtil.getDaysBetweenbyStartAndend(pretime, endtime, "yyyyMMdd hhmmss");
                    int brenum = frequencymap.get(days) == null ? 0 : frequencymap.get(days);
                    frequencymap.put(days, brenum + 1);

                    //计算最大金额
                    String totalamount = baiJiaInfoinner.getTotalamount();
                    double parseDouble = Double.parseDouble(totalamount);
                    if (parseDouble > maxmount) {
                        maxmount = parseDouble;
                    }

                    //通过总金额来计算平均金额
                    sum += parseDouble;

                    baijiainfopre = baiJiaInfoinner;
                }

                double avrmount = sum / list.size();//平均金额

                Set<Map.Entry<Integer, Integer>> entries = frequencymap.entrySet();
                int totaldayas = 0;//总时间
                for (Map.Entry<Integer, Integer> set : entries
                ) {
                    Integer fredays = set.getKey();
                    Integer frecount = set.getValue();

                    totaldayas += fredays * frecount;
                }

                int avrdays = totaldayas / list.size();//购买的平均天数


                /**
                 *   败家指数 = 支付金额平均值*0.3、最大支付金额*0.3、下单频率*0.4
                 *   支付金额平均值30分（0-20 5 20-60 10 60-100 20 100-150 30 150-200 40 200-250 60 250-350 70 350-450 80 450-600 90 600以上 100  ）
                 *   最大支付金额30分（0-20 5 20-60 10 60-200 30 200-500 60 500-700 80 700 100）
                 *   下单平率30分 （0-5 100 5-10 90 10-30 70 30-60 60 60-80 40 80-100 20 100以上的 10）
                 */

                //支付金额平均值30分（0-20 5 20-60 10 60-100 20 100-150 30 150-200 40 200-250 60 250-350 70 350-450 80 450-600 90 600以上 100 ）
                int avraoumtsoce = 0;
                if (avrmount >= 0 && avrmount < 20) {
                    avraoumtsoce = 5;
                } else if (avrmount >= 20 && avrmount < 60) {
                    avraoumtsoce = 10;
                } else if (avrmount >= 60 && avrmount < 100) {
                    avraoumtsoce = 20;
                } else if (avrmount >= 100 && avrmount < 150) {
                    avraoumtsoce = 30;
                } else if (avrmount >= 150 && avrmount < 200) {
                    avraoumtsoce = 40;
                } else if (avrmount >= 200 && avrmount < 250) {
                    avraoumtsoce = 60;
                } else if (avrmount >= 250 && avrmount < 350) {
                    avraoumtsoce = 70;
                } else if (avrmount >= 350 && avrmount < 450) {
                    avraoumtsoce = 80;
                } else if (avrmount >= 450 && avrmount < 600) {
                    avraoumtsoce = 90;
                } else if (avrmount >= 600) {
                    avraoumtsoce = 100;
                }

                //最大支付金额30分（0-20 5 20-60 10 60-200 30 200-500 60 500-700 80 700 100）
                int maxaoumtscore = 0;
                if (maxmount >= 0 && maxmount < 20) {
                    maxaoumtscore = 5;
                } else if (maxmount >= 20 && maxmount < 60) {
                    maxaoumtscore = 10;
                } else if (maxmount >= 60 && maxmount < 200) {
                    maxaoumtscore = 30;
                } else if (maxmount >= 200 && maxmount < 500) {
                    maxaoumtscore = 60;
                } else if (maxmount >= 500 && maxmount < 700) {
                    maxaoumtscore = 80;
                } else if (maxmount >= 700) {
                    maxaoumtscore = 100;
                }

                // 下单平率30分 （0-5 100 5-10 90 10-30 70 30-60 60 60-80 40 80-100 20 100以上的 10）
                int avrdaysscore = 0;
                if (avrdays >= 0 && avrdays < 5) {
                    avrdaysscore = 100;
                } else if (avrdays >= 5 && avrdays < 10) {
                    avrdaysscore = 90;
                } else if (avrdays >= 10 && avrdays < 30) {
                    avrdaysscore = 70;
                } else if (avrdays >= 30 && avrdays < 60) {
                    avrdaysscore = 60;
                } else if (avrdays >= 60 && avrdays < 80) {
                    avrdaysscore = 40;
                } else if (avrdays >= 80 && avrdays < 100) {
                    avrdaysscore = 20;
                } else if (avrdays >= 100) {
                    avrdaysscore = 10;
                }

                //败家指数 = 支付金额平均值*0.3、最大支付金额*0.3、下单频率*0.4
                double totalscore = avraoumtsoce * 0.3 + maxaoumtscore * 0.3 + avrdaysscore * 0.4;

                //结果写入hbase
                HbaseUtil.putdata("userflaginfo", userid, "baseinfo", "baijiascore", totalscore + "");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

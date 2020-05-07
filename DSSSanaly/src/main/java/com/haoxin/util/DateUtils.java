package com.haoxin.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/7 16:51
 */
public class DateUtils {
    public static String getDateby(long timestamp,String dateformat){
        Date date = new Date(timestamp);
        DateFormat dateFormat = new SimpleDateFormat(dateformat);
        String formatdate = dateFormat.format(date);
        return formatdate;
    }


    public static long getDatebyConditon(long timestamp, String dateformat) throws ParseException {
        Date datetemp = new Date(timestamp);
        DateFormat dateFormat = new SimpleDateFormat(dateformat);
        String formatdate = dateFormat.format(datetemp);
        Date date = dateFormat.parse(formatdate);
        return date.getTime();
    }
}

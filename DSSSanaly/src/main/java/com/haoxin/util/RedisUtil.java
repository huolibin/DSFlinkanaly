package com.haoxin.util;

import redis.clients.jedis.Jedis;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/4/23 16:06
 */
public class RedisUtil {
    public  static final  Jedis jedis = new Jedis("192.168.71.12",6379);
    public static String getKey(String key){
        return jedis.get(key);
    }

    public static void main(String[] args) {
        jedis.set("hello","ss");
        String value = jedis.get("hello");
        System.out.println(value);
    }
}

package com.abecedarian.demo.redis;

import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by abecedarian on 2019/3/20
 */
public class ClientDemo {

    //连接本地的 Redis 服务
    private static Jedis jedis = new Jedis("localhost");

    public static void main(String[] args) {

        //查看服务是否运行
        System.out.println("服务正在运行: " + jedis.ping());


        //设置 redis 字符串数据
        jedis.set("demokey", "www.litianle.com");
        // 获取存储的数据并输出
        System.out.println("redis 存储的字符串为: " + jedis.get("demokey"));

        //存储数据到列表中
        jedis.lpush("site-list", "abecedarian");
        jedis.lpush("site-list", "Google");
        jedis.lpush("site-list", "Taobao");
        // 获取存储的数据并输出
        List<String> list = jedis.lrange("site-list", 0, 2);
        for (int i = 0; i < list.size(); i++) {
            System.out.println("列表项为: " + list.get(i));
        }

        // 获取数据并输出
        Set<String> keys = jedis.keys("*");
        Iterator<String> it=keys.iterator() ;
        while(it.hasNext()){
            String key = it.next();
            System.out.println(key);
        }
    }

}

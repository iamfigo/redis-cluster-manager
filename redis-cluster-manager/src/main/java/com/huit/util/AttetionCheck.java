package com.huit.util;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * 根据指定的文件统计是否关注
 * java -cp redis-cluster-manager-jar-with-dependencies.jar com.huit.util.AttetionCheck host=172.20.16.48 port=5001 key=/uid.txt uid=7114937
 *
 * @author huit
 */
public class AttetionCheck {
    static int port;
    static String host;

    public static String helpInfo = "host=172.20.16.48 port=5001";
    static JedisCluster cluster;

    private static void connectCluser() {
        Set<HostAndPort> nodes = new HashSet<HostAndPort>();
        nodes.add(new HostAndPort(host, port));
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(1000);
        poolConfig.setMaxIdle(10);
        poolConfig.setMinIdle(1);
        poolConfig.setMaxWaitMillis(30000);
        poolConfig.setTestWhileIdle(true);
        cluster = new JedisCluster(nodes, poolConfig);
    }

    public static String filePath = "title", uid = "7114937";

    static FileWriter fw = null;

    public static void main(String[] args) throws Exception {
        for (String arg : args) {
            if (arg.split("=").length != 2) {
                continue;
            }
            if (arg.startsWith("key=")) {
                filePath = arg.split("=")[1];
            } else if (arg.startsWith("host=")) {
                host = arg.split("=")[1];
            } else if (arg.startsWith("port=")) {
                port = Integer.valueOf(arg.split("=")[1]);
            } else if (arg.startsWith("uid=")) {
                uid = arg.split("=")[1];
            } else {
                System.out.println("unknow arg:" + arg);
                System.out.println(helpInfo);
                System.exit(0);
            }
        }
        System.out.println("host=" + host + " port=" + port + " key:" + filePath + " uid:" + uid);
        connectCluser();
        long beginTime = System.currentTimeMillis();
        try {
            fw = new FileWriter(filePath + ".followed");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String key = "u_a_", data;
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        long index = 0;
        while ((data = br.readLine()) != null) {
            index++;
            if (null != cluster.zscore(key + data, uid)) {
                fw.write(data + "\r\n");
            }
            if (index % 10000 == 0) {
                System.out.println("check index:" + index);
            }
        }
        br.close();
        fw.close();
        System.out.println("useTime:" + (System.currentTimeMillis() - beginTime) / 1000);
    }
}

package com.huit.util;

import redis.clients.jedis.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 数据从单实例迁移到集群数据数据迁移完之后一致性检查工具
 * Created by huit on 2017/10/24.
 */
public class DataMigrationVauleCheck {
    private static String redisHost, clusterHost, keys;
    private static int redisPort, clusterPort;

    private static String helpInfo = "redisHost=10.0.6.200 redisPort=7000 clusterHost=10.0.6.200 clusterPort=6001 keys=0:hash,0:string,0:set,0:zset,0:list";

    static JedisCluster cluster;
    static Jedis old;

    public static void main(String[] args) throws IOException {
//        args = helpInfo.split(" ");
        parseArgs(args);
        Set<HostAndPort> nodes = new HashSet<HostAndPort>();
        nodes.add(new HostAndPort(clusterHost, clusterPort));
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(500);
        poolConfig.setMaxIdle(10);
        poolConfig.setMinIdle(1);
        poolConfig.setMaxWaitMillis(30000);
        poolConfig.setTestWhileIdle(true);
        cluster = new JedisCluster(nodes, 5000, 6, poolConfig);
        old = new Jedis(redisHost, redisPort);

        compareData(keys);
    }

    private static String trimValue(String value) {
        if (value.length() >= 2) {
            return value.substring(1, value.length() - 1).replace("\\\"", "\"");
        } else {
            return value;
        }
    }


    public static void compareData(String keys) {
        String[] keysInfos = keys.split(",");
        for (String keysInfo : keysInfos) {
            String[] kv = keysInfo.split(":");
            int db = Integer.valueOf(kv[0]);
            old.select(db);
            String type = old.type(kv[1]);
            String key = kv[1], clusterKey = db + "_" + key;

            if ("hash".equals(type)) {
                Map<String, String> newValue = cluster.hgetAll(clusterKey);
                Map<String, String> oldValue = old.hgetAll(db + "_" + key);
                for (Map.Entry<String, String> entry : newValue.entrySet()) {
                    String old = oldValue.get(entry.getKey());
                    if (!entry.getValue().equals(old)) {
                        System.out.println("notSync->key:" + key + " old:" + old + " new:" + entry.getValue());
                        break;
                    }
                }
                System.out.println("sync->key:" + key);
            } else if ("string".equals(type)) {
                String oldVaule = old.get(key);
                String newVaule = cluster.get(clusterKey);
                if (!oldVaule.equals(newVaule)) {
                    System.out.println("notSync->key:" + key + " old Size:" + oldVaule + " new Size:" + newVaule);
                } else {
                    System.out.println("sync->key:" + key);
                }
            } else if ("set".equals(type)) {
                long oldVaule = old.scard(key);
                long newVaule = cluster.scard(clusterKey);
                if (oldVaule != newVaule) {
                    System.out.println("notSync->key:" + key + " old Size:" + oldVaule + " new Size:" + newVaule);
                } else {
                    System.out.println("sync->key:" + key);
                }
            } else if ("list".equals(type)) {
                long oldVaule = old.llen(key);
                long newVaule = cluster.llen(clusterKey);
                if (oldVaule != newVaule) {
                    System.out.println("notSync->key:" + key + " old Size:" + oldVaule + " new Size:" + newVaule);
                } else {
                    System.out.println("sync->key:" + key);
                }
            } else if ("none".equals(type)) {
                String clusterType = cluster.type(clusterKey);
                if ("none".equals(clusterType)) {
                    System.out.println("sync->key:" + key);
                } else {
                    System.out.println("notSync->key:" + key + " old delete, new key type:" + clusterType);
                }
            } else if ("zset".equals(type)) {
                long oldVaule = old.zcard(key);
                long newVaule = cluster.zcard(clusterKey);
                if (oldVaule != newVaule) {
                    System.out.println("notSync->key:" + key + " old Size:" + oldVaule + " new Size:" + newVaule);
                } else {
                    System.out.println("sync->key:" + key);
                }
            }
        }
    }

    //TODO 16进制转中文

    public static String findStringHex(String s) {
        String v = "string\\xe4\\xb8\\xad\\xe5\\x9b\\xbd";
        return v;
    }

    // 转化十六进制编码为字符串
    public static String toStringHex(String s) {
        byte[] baKeyword = new byte[s.length() / 2];
        for (int i = 0; i < baKeyword.length; i++) {
            try {
                baKeyword[i] = (byte) (0xff & Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            s = new String(baKeyword, "utf-8");//UTF-16le:Not
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        return s;
    }

    private static void parseArgs(String[] args) throws IOException {
        for (String arg : args) {
            if (arg.split("=").length != 2) {
                continue;
            }
            if (arg.startsWith("redisHost=")) {
                redisHost = arg.split("=")[1];
            } else if (arg.startsWith("clusterHost=")) {
                clusterHost = arg.split("=")[1];
            } else if (arg.startsWith("redisPort=")) {
                redisPort = Integer.valueOf(arg.split("=")[1]);
            } else if (arg.startsWith("clusterPort=")) {
                clusterPort = Integer.valueOf(arg.split("=")[1]);
            } else if (arg.startsWith("keys=")) {
                keys = arg.split("=")[1];
            } else {
                System.out.println("helpInfo:" + helpInfo);
            }
        }
        System.out.println("input args->redisHost:" + redisHost + " redisPort:" + redisPort + " clusterHost:" + clusterHost + " clusterPort:" + clusterPort + " keys:" + keys);
    }
}

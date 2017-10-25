package com.huit.util;

import redis.clients.jedis.*;

import java.io.*;
import java.util.*;

/**
 * 数据从单实例迁移到集群数据一致性检查工具
 * Created by huit on 2017/10/24.
 */
public class DataMigrationCheck {
    private static String redisHost, clusterHost, dbs, logFilePath, ipFilter;
    private static int redisPort, clusterPort, monitorTime;

    private static String helpInfo = "redisHost=10.0.6.200 redisPort=7000 clusterHost=10.0.6.200 clusterPort=6001 ipFilter=127.0.0.1 monitorTime=100";

    static JedisCluster cluster;

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

        onlineMonitor();
    }

    private static String trimValue(String value) {
        return value.substring(1, value.length() - 1);
    }


    public static void compareData(String data) {
        if ("OK".equals(data)) {
            return;
        }
        int hostBegin = data.indexOf("[");
        int hostEnd = data.indexOf("]");

        double time;
        String db = null;
        String clientIp = null;
        String clientIpPort;
        String cmdDetail = null;
        String[] cmdInfo = null;
        if (hostBegin > 0 && hostBegin > 0) {
            time = Double.valueOf(data.substring(0, hostBegin));
            db = data.substring(hostBegin + 1, hostEnd).split(" ")[0];
            clientIpPort = data.substring(hostBegin + 1, hostEnd).split(" ")[1];
            clientIp = clientIpPort.split(":")[0];
            cmdDetail = data.substring(hostEnd + 2);
            cmdInfo = cmdDetail.split(" ");
        }

        if (null != ipFilter && !clientIp.startsWith(ipFilter)) {
            return;
        }

        if (cmdInfo.length >= 1) {
            String cmd = trimValue(cmdInfo[0]).toLowerCase();
            String oldKey = cmdInfo[1].replace("\"", "");
            String clusterKey = db + "_" + oldKey;

            if ("hmset".equals(cmd)) {
                Map<String, String> clusterValue = cluster.hgetAll(clusterKey);
                for (int i = 2; i < cmdInfo.length; i += 2) {
                    String oldValue = trimValue(cmdInfo[i + 1]);
                    String newValue = clusterValue.get(trimValue(cmdInfo[i]));
                    if (oldValue.contains("\\x")) {
                        //目前16进制未处理
                        System.out.println("syncNotSure:data:" + data + "->old:" + oldValue + " new:" + clusterKey);
                        return;
                    }
                    if (!oldValue.equals(newValue)) {
                        System.out.println("notSync:data:" + data + "->old:" + oldValue + " new:" + newValue);
                        return;
                    }
                }

                System.out.println("sync:" + data);
            } else if ("set".equals(cmd)) {
                String clusterValue = cluster.get(clusterKey);
                String oldValue = trimValue(cmdInfo[2]);
                if (oldValue.contains("\\x")) {
                    //目前16进制未处理
                    System.out.println("syncNotSure:data:" + data + "->old:" + oldValue + " new:" + clusterValue);
                    return;
                }
                if (!oldValue.equals(clusterValue)) {
                    System.out.println("notSync:" + data);
                } else {
                    System.out.println("sync:" + data);
                }
            } else if ("expire".equals(cmd)) {
                Long clusterValue = cluster.ttl(clusterKey);
                String oldValue = trimValue(cmdInfo[2]);
                if (-2 == clusterValue) {//没有key
                    System.out.println("keyNotExist:" + data);
                    return;
                }
                if (Long.valueOf(oldValue) - clusterValue >= 1) {//超过一1秒肯定不正常
                    System.out.println("notSync:" + data + "->clusterValue:" + clusterValue);
                } else {
                    System.out.println("sync:" + data);
                }
            } else if ("zadd".equals(cmd)) {
                boolean isSync = true;
                for (int i = 2; i < cmdInfo.length; i += 2) {
                    String oldValue = trimValue(cmdInfo[i + 1]);
                    if (oldValue.contains("\\x")) {
                        //目前16进制未处理
                        System.out.println("syncNotSure:data:" + data + "->old:" + oldValue + " new:" + clusterKey);
                        return;
                    }
                    if (Double.valueOf(trimValue(cmdInfo[i])) == cluster.zscore(clusterKey, oldValue)) {
                        isSync = false;
                        break;
                    }
                }
                if (!isSync) {
                    System.out.println("notSync:" + data);
                } else {
                    System.out.println("sync:" + data);
                }
            } else if ("sadd".equals(cmd)) {
                boolean isSync = true;
                for (int i = 2; i < cmdInfo.length; i++) {
                    String oldValue = trimValue(cmdInfo[i]);
                    if (oldValue.contains("\\x")) {
                        //目前16进制未处理
                        System.out.println("syncNotSure:data:" + data + "->old:" + oldValue);
                        return;
                    }
                    if (!cluster.sismember(clusterKey, oldValue)) {
                        isSync = false;
                        break;
                    }
                }
                if (!isSync) {
                    System.out.println("notSync:" + data);
                } else {
                    System.out.println("sync:" + data);
                }
            }
        } else {
            return;
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

    public static void onlineMonitor() {
        Jedis jedis = new Jedis(redisHost, Integer.valueOf(redisPort));
        JedisMonitor monitor = new JedisMonitor() {
            @Override
            public void onCommand(String command) {
                compareData(command);
            }
        };
        final long beginTime = System.currentTimeMillis();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(monitorTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    System.out.println("useTime:" + (System.currentTimeMillis() - beginTime));
                    System.exit(0);
                }
            }

        }, "monitorTimer").start();
        jedis.monitor(monitor);
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
            } else if (arg.startsWith("monitorTime=")) {
                monitorTime = Integer.valueOf(arg.split("=")[1]) * 1000;
            } else if (arg.startsWith("clusterPort=")) {
                clusterPort = Integer.valueOf(arg.split("=")[1]);
            } else if (arg.startsWith("ipFilter=")) {
                ipFilter = arg.split("=")[1];
            } else {
                System.out.println("helpInfo:" + helpInfo);
            }
        }
        System.out.println("input args->redisHost:" + redisHost + " redisPort:" + redisPort + " clusterHost:" + clusterHost + " clusterPort:" + clusterPort + " monitorTime:" + monitorTime + " logFilePath:" + logFilePath);
    }
}

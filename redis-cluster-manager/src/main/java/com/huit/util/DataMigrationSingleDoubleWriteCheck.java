package com.huit.util;

import redis.clients.jedis.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 使用方法：java -cp redis-newRedis-manager-jar-with-dependencies.jar com.huit.util.DataMigrationSingleDoubleWriteCheck args
 * 数据从单实例迁移到单实例数据双写一致性检查工具
 * 已知问题：
 * 1.中文字符比较可能不成功
 * 2.由于存在时间差，高频操作的数据可能存比较错误，使用DataMigrationValueCheck工具多检测几次看是否同步
 * <p>
 * 输入参数：
 * redisHost=10.0.6.200 单机IP
 * redisPort=6380 单机端口
 * newRedisHost=10.0.6.200 新RedisIP
 * newRedisPort=6001 新Redis端口
 * ipFilter=10.0.9.133 要过滤执行操作的机器IP
 * monitorTime=5 监控时间单位秒
 * <p>
 * 输出结果：notSync或sync，如-> sync:1531730394.453018 [0 10.0.9.133:59118] "set" "a" "a"
 * <p>
 * Created by huit on 2017/10/24.
 */
public class DataMigrationSingleDoubleWriteCheck {
    public static String redisHost, newRedisHost, ipFilter, keys, redisPwd, newRedisPwd;
    public static int redisPort, newRedisPort, monitorTime;
    public static String[] dbIndexMap = new String[16];
    public static String helpInfo = "redisHost=redis.wallet.gy.56qq.cn redisPort=6379 redisPwd=mon.wanghai newRedisHost=10.6.1.23 newRedisPort=6481 newRedisPwd=uElDG3IHZAnXhT22 ipFilter=10.0.9.133 monitorTime=5000";

    static Jedis newRedis;
    static Jedis old;

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("use default arg");
            args = helpInfo.split(" ");
        }
        ArgsParse.parseArgs(DataMigrationSingleDoubleWriteCheck.class, args, "newRedis", "old", "dbIndexMap");

        if (null != keys) {
            for (String s : keys.split(",")) {
                compareData(s);
            }
            return;
        }


        newRedis = new Jedis(newRedisHost, newRedisPort);
        if (null != newRedisPwd) {
            newRedis.auth(newRedisPwd);
        }
        old = new Jedis(redisHost, redisPort);
        if (null != redisPwd) {
            old.auth(redisPwd);
        }

        onlineMonitor();
    }

    private static String trimValue(String value) {
        if (value.length() >= 2) {
            return value.substring(1, value.length() - 1).replace("\\\"", "\"");
        } else {
            return value;
        }
    }


    public static void compareData(String data) {
        if ("OK".equals(data)) {
            return;
        }
        int hostBegin = data.indexOf("[");
        int hostEnd = data.indexOf("]");

        String db = null;
        String clientIp = null;
        String clientIpPort;
        String cmdDetail = null;
        String[] cmdInfo = null;
        if (hostBegin > 0 && hostBegin > 0) {
            db = data.substring(hostBegin + 1, hostEnd).split(" ")[0];
            clientIpPort = data.substring(hostBegin + 1, hostEnd).split(" ")[1];
            clientIp = clientIpPort.split(":")[0];
            cmdDetail = data.substring(hostEnd + 2);
            cmdInfo = cmdDetail.split(" ");
        }

        if (null == keys && null != ipFilter && !clientIp.startsWith(ipFilter)) {
            return;
        }

        if (cmdInfo.length >= 2) {
            String cmd = trimValue(cmdInfo[0]).toLowerCase();
            String oldKey = cmdInfo[1].replace("\"", "");
            String newRedisKey = buildnewRedisKey(Integer.valueOf(db), oldKey, dbIndexMap);

            if ("hmset".equals(cmd)) {
                Map<String, String> newRedisValue = newRedis.hgetAll(newRedisKey);
                for (int i = 2; i < cmdInfo.length; i += 2) {
                    String oldValue = trimValue(cmdInfo[i + 1]);
                    String newValue = newRedisValue.get(trimValue(cmdInfo[i]));
                    if (oldValue.contains("\\x")) {
                        //目前16进制未处理
                        System.out.println("syncNotSure:data:" + data + "->old:" + oldValue + " new:" + newRedisKey);
                        return;
                    }
                    if (!oldValue.equals(newValue)) {
                        System.out.println("notSync:data:" + data + "->old:" + oldValue + " new:" + newValue);
                        return;
                    }
                }

                System.out.println("sync:" + data);
            } else if ("del".equals(cmd)) {
                String newRedisValue = newRedis.type(newRedisKey);
                if (!"none".equals(newRedisValue)) {//没有被删除
                    System.out.println("notSync:" + data + "->newRedisValue:" + newRedisValue);
                    return;
                } else {
                    System.out.println("sync:" + data);
                }
            } else if ("set".equals(cmd)) {
                String newRedisValue = newRedis.get(newRedisKey);
                String oldValue = trimValue(cmdInfo[2]);
                if (oldValue.contains("\\x")) {
                    //目前16进制未处理
                    System.out.println("syncNotSure:data:" + data + "->old:" + oldValue + " new:" + newRedisValue);
                    return;
                }
                if (!oldValue.equals(newRedisValue)) {
                    System.out.println("notSync:" + data);
                } else {
                    System.out.println("sync:" + data);
                }
            } else if ("expire".equals(cmd)) {
                Long newRedisValue = newRedis.ttl(newRedisKey);
                String oldValue = trimValue(cmdInfo[2]);
                if (-2 == newRedisValue) {//没有key
                    System.out.println("keyNotExist:" + data);
                    return;
                }
                if (Long.valueOf(oldValue) - newRedisValue >= 5) {//超过一1秒肯定不正常
                    System.out.println("notSync:" + data + "->newRedisValue:" + newRedisValue);
                } else {
                    System.out.println("sync:" + data);
                }
            } else if ("zadd".equals(cmd)) {
                boolean isSync = true;
                for (int i = 2; i < cmdInfo.length; i += 2) {
                    String oldValue = trimValue(cmdInfo[i + 1]);
                    if (oldValue.contains("\\x")) {
                        //目前16进制未处理
                        System.out.println("syncNotSure:data:" + data + "->old:" + oldValue + " new:" + newRedisKey);
                        return;
                    }
                    Double oldScore = Double.valueOf(trimValue(cmdInfo[i]));
                    Double newRedisScore = newRedis.zscore(newRedisKey, oldValue);
                    if (oldScore != newRedisScore) {
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
                    if (!newRedis.sismember(newRedisKey, oldValue) && old.sismember(trimValue(cmdInfo[1]), oldValue)) {//高并发情况下可能被移出
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

    public static String buildnewRedisKey(int db, String oldKey, String[] dbIndexMap) {
        String newRedisKey = dbIndexMap[db];
        if (null == newRedisKey) {
            newRedisKey = db + "_";
        }
        newRedisKey += oldKey;
        return newRedisKey;
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
                    Thread.sleep(monitorTime * 1000);
                } catch (InterruptedException e) {
                } finally {
                    System.out.println("useTime:" + (System.currentTimeMillis() - beginTime));
                    System.exit(0);
                }
            }

        }, "monitorTimer").start();
        jedis.monitor(monitor);
    }
}

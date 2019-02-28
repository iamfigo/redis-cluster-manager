package com.huit.util;

import redis.clients.jedis.*;

import java.util.Map;

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
 * keyFilter=dpm_ 要过滤key前缀
 * monitorTime=5 监控时间单位秒
 * <p>
 * 输出结果：notSync或sync，如-> sync:1531730394.453018 [0 10.0.9.133:59118] "set" "a" "a"
 * <p>
 * Created by huit on 2017/10/24.
 */
public class DataMigrationSingleDoubleWriteCheck {
    public static String redisHost, newRedisHost, ipFilter, keyFilter, keys, redisPwd, newRedisPwd;
    public static int redisPort, newRedisPort, monitorTime;
    public static String helpInfo = "redisHost=10.6.1.53 redisPort=6379 redisPwd=mon.wanghai newRedisHost=10.6.1.23 newRedisPort=6481 newRedisPwd=uElDG3IHZAnXhT22 ipFilter= keyFilter=dpm_ monitorTime=500";

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
            String newRedisKey = oldKey;

            if (null == keys && null != keyFilter && !oldKey.startsWith(keyFilter)) {
                return;
            }

            if ("hmset".equals(cmd)) {
                Map<String, String> newRedisValue = newRedis.hgetAll(newRedisKey);
                for (int i = 2; i < cmdInfo.length; i += 2) {
                    String oldValue = HexToCn.redisString(trimValue(cmdInfo[i + 1]));
                    String newValue = newRedisValue.get(trimValue(cmdInfo[i]));
                    if (!oldValue.equals(newValue)) {
                        System.out.println("notSync->key:" + oldKey + " oldValue:" + oldValue + " newValue:" + newRedisValue);
                        return;
                    }
                }

                System.out.println("sync->key:" + oldKey);
            } else if ("del".equals(cmd)) {
                String newRedisValue = newRedis.type(newRedisKey);
                if (!"none".equals(newRedisValue)) {//没有被删除
                    System.out.println("notSync->key:" + oldKey + " oldValue:none" + " newValue:" + newRedisValue);
                    return;
                } else {
                    System.out.println("sync->key:" + oldKey);
                }
            } else if ("set".equals(cmd) || "setnx".equals(cmd)) {
                boolean isEquals = false;
                String oldValue = HexToCn.redisString(trimValue(cmdInfo[2]));
                String newRedisValue = null;
                for (int i = 0; i < 3; i++) {
                    newRedisValue = newRedis.get(newRedisKey);
                    if (oldValue.equals(newRedisValue)) {
                        isEquals = true;
                        break;
                    } else {
                        try {
                            Thread.sleep(5);
                        } catch (InterruptedException e) {
                        }
                    }
                }
                if (isEquals) {
                    System.out.println("sync->key:" + oldKey);
                } else {
                    System.out.println("notSync->key:" + oldKey + " oldValue:" + oldValue + " newValue:" + newRedisValue);
                }
            } else if ("expire".equals(cmd)) {
                Long newRedisValue = newRedis.ttl(newRedisKey);
                String oldValue = trimValue(cmdInfo[2]);
                if (Long.valueOf(oldValue) - newRedisValue >= 5) {//超过1秒肯定不正常
                    System.out.println("notSync->key:" + oldKey + " oldTtl:" + oldValue + " newTtl:" + newRedisValue);
                } else {
                    System.out.println("sync->key:" + oldKey);
                }
            } else if ("zadd".equals(cmd)) {
                boolean isSync = true;
                for (int i = 2; i < cmdInfo.length; i += 2) {
                    String oldValue = HexToCn.redisString(trimValue(cmdInfo[i + 1]));
                    Double oldScore = Double.valueOf(trimValue(cmdInfo[i]));
                    Double newRedisScore = newRedis.zscore(newRedisKey, oldValue);
                    if (oldScore != newRedisScore) {
                        isSync = false;
                        break;
                    }
                }
                if (!isSync) {
                    System.out.println("notSync->key:" + oldKey);
                } else {
                    System.out.println("sync->key:" + oldKey);
                }
            } else if ("sadd".equals(cmd)) {
                boolean isSync = true;
                for (int i = 2; i < cmdInfo.length; i++) {
                    String oldValue = HexToCn.redisString(trimValue(cmdInfo[i]));
                    if (!newRedis.sismember(newRedisKey, oldValue) && old.sismember(trimValue(cmdInfo[1]), oldValue)) {//高并发情况下可能被移出
                        isSync = false;
                        break;
                    }
                }
                if (!isSync) {
                    System.out.println("notSync->key:" + oldKey);
                } else {
                    System.out.println("sync->key:" + oldKey);
                }
            }
        } else {
            return;
        }
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
        if (null != redisPwd) {
            jedis.auth(redisPwd);
        }
        jedis.monitor(monitor);
    }
}

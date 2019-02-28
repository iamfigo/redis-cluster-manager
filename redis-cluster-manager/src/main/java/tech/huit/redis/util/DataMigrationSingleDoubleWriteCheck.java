package tech.huit.redis.util;
import redis.clients.jedis.*;

import java.util.Map;

/**
 * 使用方法：java -cp redis-cluster-manager-jar-with-dependencies.jar tech.huit.redis.util.DataMigrationSingleDoubleWriteCheck args
 * 数据从单实例迁移到单实例数据双写一致性检查工具
 * 已知问题：
 * 1.由于存在时间差，高频操作的数据可能存比较错误，可以用DataMigrationSingleValueCheck工具人多确认几次看是否同步
 * <p>
 * 输入参数：
 * redisHost=10.0.6.200 待迁移RedisIP
 * redisPort=6380 待迁移Redis端口
 * redisPwd=mon.wanghai  待迁移Redis密码
 * newRedisHost=10.0.6.200 新RedisIP
 * newRedisPort=6001 新Redis端口
 * newRedisPwd=uElDG3IHZAnXhT22 新Redis密码
 * ipFilter=10.0.9.133 要过滤执行操作的机器IP
 * keyFilter=dpm_ 要过滤key前缀
 * monitorTime=5 监控时间单位秒
 * <p>
 * 输出结果：notSync或sync，如：sync->cmd:setnx key:dpm_accountInfo_200011420515_201
 * <p>
 * Created by huit on 2017/10/24.
 */
public class DataMigrationSingleDoubleWriteCheck {
    public static String redisHost, newRedisHost, ipFilter, keyFilter, redisPwd, newRedisPwd;
    public static int redisPort, newRedisPort, monitorTime;
    public static String helpInfo = "redisHost=10.6.1.53 redisPort=6379 redisPwd=mon.wanghai newRedisHost=10.6.1.23 newRedisPort=6481 newRedisPwd=uElDG3IHZAnXhT22 ipFilter= keyFilter=dpm_ monitorTime=500";

    static Jedis newRedis;
    static Jedis oldRedis;

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("use default arg");
            args = helpInfo.split(" ");
        }
        ArgsParse.parseArgs(DataMigrationSingleDoubleWriteCheck.class, args, "newRedis", "oldRedis", "lastDbIndex");

        newRedis = new Jedis(newRedisHost, newRedisPort);
        if (null != newRedisPwd) {
            newRedis.auth(newRedisPwd);
        }
        oldRedis = new Jedis(redisHost, redisPort);
        if (null != redisPwd) {
            oldRedis.auth(redisPwd);
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

    private static int lastDbIndex = 0;

    public static void compareData(String data) {
        if ("OK".equals(data)) {
            return;
        }
        int hostBegin = data.indexOf("[");
        int hostEnd = data.indexOf("]");

        int db = 0;
        String clientIp = null;
        String clientIpPort;
        String cmdDetail = null;
        String[] cmdInfo = null;
        if (hostBegin > 0 && hostBegin > 0) {
            db = Integer.valueOf(data.substring(hostBegin + 1, hostEnd).split(" ")[0]);
            clientIpPort = data.substring(hostBegin + 1, hostEnd).split(" ")[1];
            clientIp = clientIpPort.split(":")[0];
            cmdDetail = data.substring(hostEnd + 2);
            cmdInfo = cmdDetail.split(" ");
        }

        if (null != ipFilter && !clientIp.startsWith(ipFilter)) {
            return;
        }

        if ("\"SELECT\"".equalsIgnoreCase(cmdInfo[0]) || cmdInfo.length < 2) {
            return;
        }

        String cmd = trimValue(cmdInfo[0]).toLowerCase();
        String key = cmdInfo[1].replace("\"", "");
        if (null != keyFilter && !key.startsWith(keyFilter)) {
            return;
        }

        if (lastDbIndex != db) {
            oldRedis.select(db);
            newRedis.select(db);
            lastDbIndex = db;
        }

        if ("hmset".equals(cmd)) {
            Map<String, String> newRedisValue = newRedis.hgetAll(key);
            for (int i = 2; i < cmdInfo.length; i += 2) {
                String oldValue = HexToCn.redisString(trimValue(cmdInfo[i + 1]));
                String newValue = newRedisValue.get(trimValue(cmdInfo[i]));
                if (!oldValue.equals(newValue)) {
                    printSyncResult(clientIp, cmd, key, false, oldValue, newValue);
                    return;
                }
            }
            printSyncResult(clientIp, cmd, key);
        } else if ("del".equals(cmd)) {
            boolean isEquals = false;
            for (int i = 0; i < 5; i++) {
                String newRedisValue = newRedis.type(key);
                if ("none".equals(newRedisValue)) {
                    isEquals = true;
                    break;
                }
                waitMillis(20);
            }
            printSyncResult(clientIp, cmd, key, isEquals, null, null);
        } else if ("set".equals(cmd) || "setnx".equals(cmd)) {
            boolean isEquals = false;
            String oldValue = HexToCn.redisString(trimValue(cmdInfo[2]));
            String newRedisValue = null;
            for (int i = 0; i < 5; i++) {
                newRedisValue = newRedis.get(key);
                if (oldValue.equals(newRedisValue)) {
                    isEquals = true;
                    break;
                } else {
                    waitMillis(20);
                    oldValue = oldRedis.get(key);
                }
            }
            printSyncResult(clientIp, cmd, key, isEquals, oldValue, newRedisValue);
        } else if ("expire".equals(cmd)) {
            Long newRedisValue = newRedis.ttl(key);
            String oldValue = trimValue(cmdInfo[2]);
            if (Long.valueOf(oldValue) - newRedisValue >= 5) {//超过1秒肯定不正常
                printSyncResult(clientIp, cmd, key, true, oldValue, newRedisValue.toString());
            } else {
                printSyncResult(clientIp, cmd, key);
            }
        } else if ("zadd".equals(cmd)) {
            boolean isSync = true;
            for (int i = 2; i < cmdInfo.length; i += 2) {
                String oldValue = HexToCn.redisString(trimValue(cmdInfo[i + 1]));
                Double oldScore = Double.valueOf(trimValue(cmdInfo[i]));
                Double newRedisScore = newRedis.zscore(key, oldValue);
                if (oldScore != newRedisScore) {
                    isSync = false;
                    break;
                }
            }
            printSyncResult(clientIp, cmd, key, isSync, null, null);
        } else if ("sadd".equals(cmd)) {
            boolean isSync = true;
            for (int i = 2; i < cmdInfo.length; i++) {
                String oldValue = HexToCn.redisString(trimValue(cmdInfo[i]));
                if (!newRedis.sismember(key, oldValue) && oldRedis.sismember(trimValue(cmdInfo[1]), oldValue)) {//高并发情况下可能被移出
                    isSync = false;
                    break;
                }
            }
            printSyncResult(clientIp, cmd, key, isSync, null, null);
        }
    }

    private static void waitMillis(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
        }
    }

    private static void printSyncResult(String clientIp, String cmd, String key) {
        printSyncResult(clientIp, cmd, key, true);
    }

    private static void printSyncResult(String clientIp, String cmd, String key, boolean isEquals) {
        printSyncResult(clientIp, cmd, key, isEquals, null, null);
    }

    private static void printSyncResult(String clientIp, String cmd, String key, boolean isEquals, String oldValue, String newRedisValue) {
        if (isEquals) {
            System.out.println("sync->clientIp:" + clientIp + " cmd:" + cmd + " key:" + key);
        } else {
            System.out.println("notSync->clientIp:" + clientIp + " cmd:" + cmd + " key:" + key + " oldValue:" + oldValue + "newValue:" + newRedisValue);
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

package com.huit.util;

import redis.clients.jedis.Jedis;
import java.util.Map;

/**
 * 使用方法：java -cp redis-cluster-manager-jar-with-dependencies.jar com.huit.util.DataMigrationSingleValueCheck args
 * 数据从单实例迁移到新机器数据迁移完之后一致性检查工具
 * redisHost=10.0.6.200 单机IP
 * redisPort=6380 单机端口
 * newRedisHost=10.0.6.200 新RedisIP
 * newRedisPort=6001 新Redis端口
 * keys=hash,0#string,1#set,0#zset,0#list 要检查的key列表，格式：db0#key,db1#key2
 * <p>
 * 输出：sync->key:hash
 * Created by huit on 2017/10/24.
 */
public class DataMigrationSingleValueCheck {
    public static String redisHost, newRedisHost, keys, redisPwd, newRedisPwd;
    public static int redisPort, newRedisPort;
    public static String helpInfo = "redisHost=10.6.1.53 redisPort=6379 redisPwd=mon.wanghai newRedisHost=10.6.1.23 newRedisPort=6481 newRedisPwd=uElDG3IHZAnXhT22 ipFilter= keys=0#dpm_accountStatus_200024642093_201";

    static Jedis newRedis;
    static Jedis old;

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("use default arg");
            args = helpInfo.split(" ");
        }
        ArgsParse.parseArgs(DataMigrationSingleValueCheck.class, args, "newRedis", "old");

        newRedis = new Jedis(newRedisHost, newRedisPort);
        if (null != newRedisPwd) {
            newRedis.auth(newRedisPwd);
        }
        old = new Jedis(redisHost, redisPort);
        if (null != redisPwd) {
            old.auth(redisPwd);
        }
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
            String[] kv = keysInfo.split("#");
            int db = Integer.valueOf(kv[0]);
            old.select(db);
            newRedis.select(db);
            String type = old.type(kv[1]);
            String key = kv[1];
            String newKey = key;


            if ("hash".equals(type)) {
                Map<String, String> newValue = newRedis.hgetAll(newKey);
                Map<String, String> oldValue = old.hgetAll(key);
                boolean isSync = true;
                for (Map.Entry<String, String> entry : newValue.entrySet()) {
                    String filed = entry.getKey();
                    String old = oldValue.get(filed);
                    if (!entry.getValue().equals(old)) {
                        isSync = false;
                        System.out.println("notSyncHash->key:" + key + " filed:" + filed + " old:" + old + " new:" + entry.getValue());
                        break;
                    }
                }
                if (isSync) {
                    System.out.println("sync->key:" + key);
                }
            } else if ("string".equals(type)) {
                String oldVaule = old.get(key);
                String newVaule = newRedis.get(newKey);
                if (!oldVaule.equals(newVaule)) {
                    System.out.println("notSyncsString->key:" + key + " oldValue:" + oldVaule + " newValue:" + newVaule);
                } else {
                    System.out.println("sync->key:" + key);
                }
            } else if ("set".equals(type)) {
                long oldVaule = old.scard(key);
                long newVaule = newRedis.scard(newKey);
                if (oldVaule != newVaule) {
                    System.out.println("notSyncSet->key:" + key + " oldSize:" + oldVaule + " newSize:" + newVaule);
                } else {
                    System.out.println("sync->key:" + key);
                }
            } else if ("list".equals(type)) {
                long oldVaule = old.llen(key);
                long newVaule = newRedis.llen(newKey);
                if (oldVaule != newVaule) {
                    System.out.println("notSyncList->key:" + key + " oldSize:" + oldVaule + " newSize:" + newVaule);
                } else {
                    System.out.println("sync->key:" + key);
                }
            } else if ("none".equals(type)) {
                String clusterType = newRedis.type(newKey);
                if ("none".equals(clusterType)) {
                    System.out.println("sync->key:" + key);
                } else {
                    System.out.println("notSync->key:" + key + " old delete, new key type:" + clusterType);
                }
            } else if ("zset".equals(type)) {
                long oldVaule = old.zcard(key);
                long newVaule = newRedis.zcard(newKey);
                if (oldVaule != newVaule) {
                    System.out.println("notSyncZset->key:" + key + " old Size:" + oldVaule + " new Size:" + newVaule);
                } else {
                    System.out.println("sync->key:" + key);
                }
            }
        }
    }
}
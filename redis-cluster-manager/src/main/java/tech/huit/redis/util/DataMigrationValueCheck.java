package tech.huit.redis.util;

import redis.clients.jedis.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 使用方法：java -cp redis-cluster-manager-jar-with-dependencies.jar DataMigrationValueCheck args
 * 数据从单实例迁移到集群数据数据迁移完之后一致性检查工具
 * redisHost=10.0.6.200 单机IP
 * redisPort=6380 单机端口
 * clusterHost=10.0.6.200 集群IP
 * clusterPort=6001 集群端口
 * dbMap=0->shop,1->good 数据映射关系，如0映射为shop,如果指定默认 0:key映射为0_key
 * keys=hash,0#string,1#set,0#zset,0#list 要检查的key列表，格式：db#key,db1#key2
 * <p>
 * 输出：sync->key:hash
 * Created by huit on 2017/10/24.
 */
public class DataMigrationValueCheck {
    public static String redisHost, clusterHost, keys;
    public static int redisPort, clusterPort;
    /**
     * db映射成集群的前缀
     */
    public static Map<String, String> dbMap = new HashMap<String, String>();
    public static String[] dbIndexMap = new String[16];

    public static String helpInfo = "redisHost=10.0.6.200 redisPort=6380 clusterHost=10.0.6.200 clusterPort=6001 dbMap=0->shop,1->good keys=0#key,0#string,0#set,0#zset,0#list";

    static JedisCluster cluster;
    static Jedis old;

    public static void main(String[] args) throws Exception {
        if(args.length == 0){
            System.out.println("use default arg");
            args = helpInfo.split(" ");
        }
        ArgsParse.parseArgs(DataMigrationValueCheck.class, args, "cluster", "old", "dbIndexMap");
        for (Map.Entry<String, String> entry : dbMap.entrySet()) {
            dbIndexMap[Integer.valueOf(entry.getKey())] = entry.getValue();
        }

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
            String[] kv = keysInfo.split("#");
            int db = Integer.valueOf(kv[0]);
            old.select(db);
            String type = old.type(kv[1]);
            String key = kv[1];
            String clusterKey = DataMigrationDoubleWriteCheck.buildClusterKey(db, key, dbIndexMap);


            if ("hash".equals(type)) {
                Map<String, String> newValue = cluster.hgetAll(clusterKey);
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
                String newVaule = cluster.get(clusterKey);
                if (!oldVaule.equals(newVaule)) {
                    System.out.println("notSyncsString->key:" + key + " old Size:" + oldVaule + " new Size:" + newVaule);
                } else {
                    System.out.println("sync->key:" + key);
                }
            } else if ("set".equals(type)) {
                long oldVaule = old.scard(key);
                long newVaule = cluster.scard(clusterKey);
                if (oldVaule != newVaule) {
                    System.out.println("notSyncSet->key:" + key + " old Size:" + oldVaule + " new Size:" + newVaule);
                } else {
                    System.out.println("sync->key:" + key);
                }
            } else if ("list".equals(type)) {
                long oldVaule = old.llen(key);
                long newVaule = cluster.llen(clusterKey);
                if (oldVaule != newVaule) {
                    System.out.println("notSyncList->key:" + key + " old Size:" + oldVaule + " new Size:" + newVaule);
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
                    System.out.println("notSyncZset->key:" + key + " old Size:" + oldVaule + " new Size:" + newVaule);
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
}

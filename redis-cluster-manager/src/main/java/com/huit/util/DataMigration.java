package com.huit.util;

import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 数据从单实例迁移到集群，已知问题：超大Key如BloomFilter迁移可能导致工具内存溢出
 * 使用方法：java -cp redis-cluster-manager-jar-with-dependencies.jar com.huit.util.DataMigration args
 * <p>
 * redisHost=10.0.6.200 单机IP
 * redisPort=6380 单机端口
 * clusterHost=10.0.6.200 集群IP
 * clusterPort=6001 集群端口
 * dbs=0,1,2/all 要迁移的db
 * dbMap=0->shop,1->good db映射
 * logFilePath=/Users/huit/migration.log" 迁移日志
 * exclude=deviceBloomFilter:bits,key2 要排除的key
 * debug=true/false 是否调试模式，如果是在控制台打印输出
 * <p>
 * Created by huit on 2017/10/20.
 */
public class DataMigration {
    public static String redisHost, clusterHost, dbs, logFilePath, keyPre = "*";
    public static int redisPort, clusterPort;
    public static boolean debug = false;
    public static Set<String> dbsSet = new HashSet<String>();//要签移的db
    public static Set exclude;
    /**
     * db映射成集群的前缀
     */
    public static Map<String, String> dbMap = new HashMap<String, String>();
    public static String[] dbIndexMap = new String[16];


    private static String getValue(String data, String key) {
        for (String s : data.split(",")) {
            if (s.startsWith(key)) {
                return s.split("=")[1];
            }
        }
        return null;
    }

    private static Long getValueLong(String data, String key) {
        String value = getValue(data, key);
        if (null != value) {
            return Long.parseLong(value);
        }
        return null;
    }

    private static Map<String, Long> dbSize = new TreeMap<String, Long>();

    private static String helpInfo = "redisHost=10.0.6.200 redisPort=6380 clusterHost=localhost clusterPort=6001 dbs=0 dbMap=0->shop,1->good logFilePath=/Users/huit/migration.log debug=true exclude=deviceBloomFilter:bits,key2";

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = helpInfo.split(" ");
        }
        ArgsParse.parseArgs(DataMigration.class, args, "dbMap", "dbIndexMap");
        for (Map.Entry<String, String> entry : dbMap.entrySet()) {
            dbIndexMap[Integer.valueOf(entry.getKey())] = entry.getValue();
        }
        dbsSet.addAll(Arrays.asList(dbs.split(",")));
        if (null != logFilePath) {
            bw = new BufferedWriter(new FileWriter(logFilePath));
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        bw.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }));
        }
        printMigrationInfo();

        Set<HostAndPort> nodes = new HashSet<HostAndPort>();
        nodes.add(new HostAndPort(clusterHost, clusterPort));
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(500);
        poolConfig.setMaxIdle(10);
        poolConfig.setMinIdle(1);
        poolConfig.setMaxWaitMillis(30000);
        poolConfig.setTestWhileIdle(true);
        final JedisCluster cluster = new JedisCluster(nodes, 5000, 6, poolConfig);

        long beginTime = System.currentTimeMillis();
        List<Thread> threadsAlive = new ArrayList<Thread>();
        for (final Map.Entry<String, Long> db : dbSize.entrySet()) {
            final String dbKey = db.getKey();
            System.out.println("start to migration db:" + dbKey + "...");
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    migrationKeyOneHost(new Jedis(redisHost, redisPort), Integer.valueOf(dbKey.substring("db".length())), db.getValue(), keyPre, cluster);
                }
            }, "migration-db-" + dbKey);
            threadsAlive.add(thread);
            thread.start();
        }

        RedisClusterManager.waitThread(threadsAlive);

        System.out.println("scanTotalCount->" + scanTotalCount + " migrationTotalCount->" + migrationTotalCount + " errorTotalCount->" + errorTotalCount + " useTime->" + ((System.currentTimeMillis() - beginTime) / 1000) + "s");
    }

    private static void printMigrationInfo() {
        Jedis jedis = new Jedis(redisHost, redisPort);
        String info = jedis.info("Keyspace");
        System.out.println("migration db info begin");
        String[] infos = info.split("\r\n");
        for (String s : infos) {
            if (s.startsWith("#")) {
                continue;
            }
            String[] dbInfo = s.split(":");

            if ("all".equals(dbs) || dbsSet.contains(dbInfo[0].replace("db", ""))) {
                dbSize.put(dbInfo[0], getValueLong(dbInfo[1], "keys"));
                System.out.println(s);
            }
        }
        jedis.close();
        System.out.println("migration db info end");
    }

    private static ScanParams sp = new ScanParams();

    static {
        sp.count(10000);
    }

    static AtomicLong scanTotalCount = new AtomicLong(), migrationTotalCount = new AtomicLong(), errorTotalCount = new AtomicLong();


    /**
     * 按key导出数据
     */
    public static void migrationKeyOneHost(Jedis nodeCli, int db, Long dbKeySize, String keyPre, JedisCluster cluster) {
        String[] exportKeyPre = keyPre.split(",");
        nodeCli.select(db);
        long beginTime = System.currentTimeMillis();
        String cursor = "0";
        long thisScanCount = 0, thisMigrationCount = 0, thisErrorCount = 0;
        do {
            ScanResult<String> keys = nodeCli.scan(cursor, sp);
            cursor = keys.getStringCursor();
            List<String> result = keys.getResult();
            for (String key : result) {
                thisScanCount++;
                if (thisScanCount % 5000 == 0) {
                    System.out.println("migration db:" + db + " thisScanSize:" + thisScanCount + "/" + dbKeySize + " thisMigrationSize:" + thisMigrationCount
                            + " thisErrorCount:" + thisErrorCount + " thisUseTime:" + (System.currentTimeMillis() - beginTime) / 1000 + "s)");
                }

                boolean isExport = false;
                for (String keyExport : exportKeyPre) {
                    if ("*".equals(keyExport) || key.startsWith(keyExport)) {
                        isExport = true;
                        break;
                    }
                }
                if (!isExport || exclude.contains(key)) {
                    continue;
                }

                JSONObject json = new JSONObject();
                json.put("key", key);
                json.put("db", db);
                String clusterKey = DataMigrationDoubleWriteCheck.buildClusterKey(db, key, dbIndexMap);
                String keyType = nodeCli.type(key);
                json.put("type", keyType);
                long ttl = nodeCli.ttl(key);//读取key数据之前先得到key过期时间，防止在写数据的过程中出现数据过期
                if (debug) {
                    System.out.println(json);
                }
                if ("hash".equals(keyType)) {//覆盖
                    //nodeCli.hgetAll(key);//大key read time out
                    //cluster.hmset(clusterKey, value);//大key ERR Protocol error: invalid multibulk length
                    Map value = new HashMap();
                    String hcursor = "0";
                    do {
                        ScanResult<Map.Entry<String, String>> hscanResult = nodeCli.hscan(key, hcursor, sp);
                        hcursor = hscanResult.getStringCursor();
                        Map temp = new HashMap();
                        for (Map.Entry<String, String> entry : hscanResult.getResult()) {
                            temp.put(entry.getKey(), entry.getValue());
                        }
                        if (temp.size() > 0) {
                            try {
                                cluster.hmset(clusterKey, temp);
                                value.putAll(temp);
                            } catch (Throwable e) {
                                thisErrorCount++;
                                System.out.println("migrationError->key:" + key + " type:" + keyType + " value:" + temp);
                                e.printStackTrace();
                            }
                        }
                    } while (!"0".equals(hcursor));
                    json.put("value", value);
                } else if ("string".equals(keyType)) {//覆盖
                    String value = nodeCli.get(key);
                    try {
                        if (null != value && value.length() > 0) {
                            cluster.set(clusterKey, value);
                        }
                    } catch (Throwable e) {
                        thisErrorCount++;
                        System.out.println("migrationError->key:" + key + " value:" + value);
                        e.printStackTrace();
                    }
                    json.put("value", value);
                } else if ("list".equals(keyType)) {//删除再覆盖
                    cluster.del(clusterKey);//list不能合并，必须要先删除老的key
                    int readSize, readCount = 10000;//大list且增删频繁导致分页处数据丢失或重复
                    long start = 0, end = start + readCount;
                    List<String> value = new ArrayList<String>();
                    do {
                        List<String> data = nodeCli.lrange(key, start, end);
                        readSize = data.size();
                        List<String> temp = new ArrayList<String>();
                        for (int i = 0; i < readSize; i++) {
                            String valueStr = data.get(i);
                            temp.add(valueStr);
                        }
                        try {
                            if (temp.size() > 0) {
                                cluster.rpush(clusterKey, temp.toArray(new String[0]));
                                value.addAll(temp);
                            }
                        } catch (Throwable e) {
                            thisErrorCount++;
                            System.out.println("migrationError->key:" + key + " type:" + keyType + " value:" + temp);
                            e.printStackTrace();
                        }
                        start = end + 1;
                        end += readSize;
                    } while (readSize == readCount + 1);//-1 is the last element of the list
                    json.put("value", value);
                } else if ("set".equals(keyType)) {//新新加
                    String scursor = "0";
                    List<String> value = new ArrayList<String>();
                    do {
                        ScanResult<String> sscanResult = nodeCli.sscan(key, scursor, sp);
                        scursor = sscanResult.getStringCursor();
                        List<String> temp = new ArrayList<String>(sscanResult.getResult().size());
                        for (String data : sscanResult.getResult()) {
                            temp.add(data);
                        }
                        try {
                            if (temp.size() > 0) {
                                cluster.sadd(clusterKey, temp.toArray(new String[0]));
                                value.addAll(temp);
                            }
                        } catch (Throwable e) {
                            thisErrorCount++;
                            System.out.println("migrationError->key:" + key + " value:" + temp);
                            e.printStackTrace();
                        }
                    } while (!"0".equals(scursor));
                    json.put("value", value);
                } else if ("zset".equals(keyType)) {
                    String zcursor = "0";
                    List<Map<String, Double>> value = new ArrayList();
                    do {
                        ScanResult<Tuple> sscanResult = nodeCli.zscan(key, zcursor, sp);
                        zcursor = sscanResult.getStringCursor();
                        Map<String, Double> temp = new HashMap<String, Double>();
                        for (Tuple data : sscanResult.getResult()) {
                            temp.put(data.getElement(), data.getScore());
                        }
                        try {
                            if (null != temp && temp.size() > 0) {
                                cluster.zadd(clusterKey, temp);
                                value.add(temp);
                            }
                        } catch (Throwable e) {
                            thisErrorCount++;
                            System.out.println("migrationError->key:" + key + " value:" + temp);
                            e.printStackTrace();
                        }
                    } while (!"0".equals(zcursor));
                    json.put("value", value);
                } else if ("none".equals(keyType)) {//刚好过期的key,可以不用管
                } else {
                    System.out.println("unknow keyType:" + keyType + " key:" + key);
                }
                if (ttl > 0) {//统一设置ttl时间
                    json.put("ttl", ttl);
                    cluster.expire(clusterKey, (int) ttl);
                }
                thisMigrationCount++;
                writeLog(json);
            }
        } while ((!"0".equals(cursor)));

        System.out.println("migration db:" + db + " success thisScanCount->" + thisScanCount + " thisMigrationCount->" + thisMigrationCount + " thisErrorCount->" + thisErrorCount + " expireCount->" + (dbKeySize - thisMigrationCount) + " useTime->" + ((System.currentTimeMillis() - beginTime) / 1000) + "s");
        scanTotalCount.addAndGet(thisScanCount);
        migrationTotalCount.addAndGet(thisMigrationCount);
        errorTotalCount.addAndGet(thisErrorCount++);
        nodeCli.close();
    }

    static BufferedWriter bw = null;

    private synchronized static void writeLog(JSONObject json) {
        if (null == bw) {
            return;
        }
        try {
            bw.write(json.toJSONString());
            bw.write('\r');
            bw.write('\n');
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("writeLogError->" + json);
        }
    }
}

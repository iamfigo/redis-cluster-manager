package com.huit.util;

import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

/**
 * 数据从单实例迁移到集群
 * Created by huit on 2017/10/20.
 */
public class DataMigration {
    private static String redisHost, clusterHost, dbs, logFilePath, keyPre = "*";
    private static int redisPort, clusterPort;
    private static Set<String> dbsSet = new HashSet<String>();//要签移的db

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

    private static String helpInfo = "redisHost=10.0.6.200 redisPort=6379 clusterHost=10.0.6.200 clusterPort=6000 dbs=db0,db1,db2 logFilePath=d:/migration.log";

    public static void main(String[] args) throws IOException {
//        args = helpInfo.split(" ");
        parseArgs(args);
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
        for (final Map.Entry<String, Long> db : dbSize.entrySet()) {
            final String dbKey = db.getKey();
            System.out.println("start to migration db:" + dbKey + "...");
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    migrationKeyOneHost(new Jedis(redisHost, redisPort), Integer.valueOf(dbKey.substring("db".length())), db.getValue(), keyPre, cluster);
                }
            }, "migration-db-" + dbKey);
            thread.start();
        }

        System.out.println("scanTotalCount->" + scanTotalCount + " migrationCount->" + migrationTotalCount + " useTime->" + ((System.currentTimeMillis() - beginTime) / 1000) + "s");
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

            if ("all".equals(dbs) || dbsSet.contains(dbInfo[0])) {
                dbSize.put(dbInfo[0], getValueLong(dbInfo[1], "keys"));
                System.out.println(s);
            }
        }
        jedis.close();
        System.out.println("migration db info end");
    }

    static ScanParams sp = new ScanParams();

    static {
        sp.count(10000);
    }

    static long scanTotalCount = 0, migrationTotalCount = 0;


    /**
     * 按key导出数据
     */
    public static void migrationKeyOneHost(Jedis nodeCli, int db, Long dbKeySize, String keyPre, JedisCluster cluster) {
        String[] exportKeyPre = keyPre.split(",");
        nodeCli.select(db);
        long scanCount = 0, migrationCount = 0;
        long beginTime = System.currentTimeMillis();
        String cursor = "0";
        long thisScanSize = 0, thisExportSize = 0;
        do {
            ScanResult<String> keys = nodeCli.scan(cursor, sp);
            cursor = keys.getStringCursor();
            List<String> result = keys.getResult();
            for (String key : result) {
                thisScanSize++;
                scanCount++;
                if (thisScanSize % 1000 == 0) {
                    System.out.println("migration db:" + db + "thisScanSize:" + thisScanSize + "/" + dbKeySize + " thisExportSize:" + thisExportSize
                            + " totalUseTime:" + (System.currentTimeMillis() - beginTime) / 1000 + "s)");
                }

                boolean isExport = false;
                for (String keyExport : exportKeyPre) {
                    if ("*".equals(keyExport) || key.startsWith(keyExport)) {
                        isExport = true;
                        break;
                    }
                }
                if (!isExport) {
                    continue;
                }

                JSONObject json = new JSONObject();
                json.put("key", key);
                String clusterKey = db + "_" + key;
                String keyType = nodeCli.type(key);
                json.put("type", keyType);
                long ttl = nodeCli.ttl(key);//读取key数据之前先得到key过期时间，防止在写数据的过程中数据过期
                if ("hash".equals(keyType)) {
                    Map<String, String> value = nodeCli.hgetAll(key);
                    if (null == value || value.size() == 0) {//数据刚好过期
                        System.out.println("migrationDataExpired->key:" + key + " type:" + keyType);
                        continue;
                    }
                    json.put("value", value);
                    try {
                        cluster.hmset(clusterKey, value);
                    } catch (Throwable e) {
                        System.out.println("migrationError->key:" + key + " value:" + value);
                        e.printStackTrace();
                    }
                } else if ("string".equals(keyType)) {
                    String value = nodeCli.get(key);
                    if (null == value || value.length() == 0) {//数据刚好过期
                        System.out.println("migrationDataExpired->key:" + key + " type:" + keyType);
                        continue;
                    }
                    json.put("value", value);
                    try {
                        cluster.set(clusterKey, value);
                    } catch (Throwable e) {
                        System.out.println("migrationError->key:" + key + " value:" + value);
                        e.printStackTrace();
                    }
                } else if ("list".equals(keyType)) {
//                    int readSize, readCount = 3;//大list且增删频繁导致分页处数据丢失或重复
//                    long start = 0, end = start + readCount;
//                    List<String> value = new ArrayList<String>();
//                    do {
//                        List<String> data = nodeCli.lrange(key, start, end);
//                        readSize = data.size();
//                        for (int i = 0; i < readSize; i++) {
//                            String valueStr = data.get(i);
//                            value.add(valueStr);
//                            cluster.rpush(clusterKey, valueStr);
//                        }
//                        start = end + 1;
//                        end += readSize;
//                    } while (readSize == readCount + 1);//-1 is the last element of the list
                    List<String> value = nodeCli.lrange(key, 0, -1);
                    if (null == value || value.size() == 0) {//数据刚好过期
                        System.out.println("migrationDataExpired->key:" + key + " type:" + keyType);
                        continue;
                    }
                    json.put("value", value);
                    try {
                        cluster.rpush(clusterKey, value.toArray(new String[0]));
                    } catch (Throwable e) {
                        System.out.println("migrationError->key:" + key + " value:" + value);
                        e.printStackTrace();
                    }
                } else if ("set".equals(keyType)) {
                    String scursor = "0";
                    List<String> value = new ArrayList<String>();
                    boolean isFirst = true;
                    do {
                        ScanResult<String> sscanResult = nodeCli.sscan(key, scursor, sp);
                        scursor = sscanResult.getStringCursor();
                        List<String> tmp = new ArrayList<String>(sscanResult.getResult().size());
                        for (String data : sscanResult.getResult()) {
                            value.add(data);
                            tmp.add(data);
                        }
                        try {
                            if (isFirst && (null == tmp || tmp.size() == 0)) {//第一次数据刚好过期
                                System.out.println("migrationDataExpired->key:" + key + " type:" + keyType);
                                continue;
                            }
                            if (null == tmp || tmp.size() > 0) {
                                cluster.sadd(clusterKey, tmp.toArray(new String[0]));
                            }
                            isFirst = false;
                        } catch (Throwable e) {
                            System.out.println("migrationError->key:" + key + " value:" + tmp);
                            e.printStackTrace();
                        }
                    } while (!"0".equals(scursor));

                    json.put("value", value);
                } else if ("zset".equals(keyType)) {
                    String zcursor = "0";
                    List<Map<String, Double>> value = new ArrayList();
                    boolean isFirst = true;
                    do {
                        ScanResult<Tuple> sscanResult = nodeCli.zscan(key, zcursor, sp);
                        zcursor = sscanResult.getStringCursor();
                        Map<String, Double> dataJson = new HashMap<String, Double>();
                        for (Tuple data : sscanResult.getResult()) {
                            dataJson.put(data.getElement(), data.getScore());
                            value.add(dataJson);
                        }
                        try {
                            if (isFirst && (null == dataJson || dataJson.size() == 0)) {//第一次数据刚好过期
                                System.out.println("migrationDataExpired->key:" + key + " type:" + keyType);
                                continue;
                            }
                            if (null == dataJson || dataJson.size() > 0) {
                                cluster.zadd(clusterKey, dataJson);
                            }
                            isFirst = false;
                        } catch (Throwable e) {
                            System.out.println("migrationError->key:" + key + " value:" + dataJson);
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
                thisExportSize++;
                migrationCount++;

                writeLog(json);
            }
        } while ((!"0".equals(cursor)));

        System.out.println("migration db:" + db + " success, scanCount->" + scanCount + " expireCount->" + (dbKeySize - migrationCount) + " migrationCount->" + migrationCount + " useTime->" + ((System.currentTimeMillis() - beginTime) / 1000) + "s");
        scanTotalCount += scanCount;
        migrationTotalCount += migrationCount;
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
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("writeLogError->" + json);
        }
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
            } else if (arg.startsWith("dbs=")) {
                dbs = arg.split("=")[1];
                dbsSet.addAll(Arrays.asList(dbs.split(",")));
            } else if (arg.startsWith("redisPort=")) {
                redisPort = Integer.valueOf(arg.split("=")[1]);
            } else if (arg.startsWith("clusterPort=")) {
                clusterPort = Integer.valueOf(arg.split("=")[1]);
            } else if (arg.startsWith("logFilePath=")) {
                logFilePath = arg.split("=")[1];
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
            } else if (arg.startsWith("keyPre=")) {
                keyPre = arg.split("=")[1];
            } else {
                System.out.println("helpInfo:" + helpInfo);
            }
        }
        System.out.println("input args->redisHost:" + redisHost + " redisPort:" + redisPort + " clusterHost:" + clusterHost + " clusterPort:" + clusterPort + " dbs:" + dbs + " logFilePath:" + logFilePath + " keyPre:" + keyPre);
    }
}

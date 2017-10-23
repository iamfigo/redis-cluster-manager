package com.huit.util;

import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * 数据从单实例迁移到集群
 * Created by huit on 2017/10/20.
 */
public class DataMigration {
    private static String redisHost, clusterHost, dbs, logFilePath, keyPre = "*";
    private static int redisPort, clusterPort;

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

        Jedis jedis = new Jedis(redisHost, redisPort);
        String info = jedis.info("Keyspace");
        String[] infos = info.split("\r\n");
        for (String s : infos) {
            if (s.startsWith("#")) {
                continue;
            }
            String[] dbInfo = s.split(":");

            if ("all".equals(dbs) || dbs.contains(dbInfo[0])) {
                dbSize.put(dbInfo[0], getValueLong(dbInfo[1], "keys"));
                System.out.println(s);
            }
        }

        Set<HostAndPort> nodes = new HashSet<HostAndPort>();
        nodes.add(new HostAndPort(clusterHost, clusterPort));
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(500);
        poolConfig.setMaxIdle(10);
        poolConfig.setMinIdle(1);
        poolConfig.setMaxWaitMillis(30000);
        poolConfig.setTestWhileIdle(true);
        JedisCluster cluster = new JedisCluster(nodes, 5000, 6, poolConfig);

        long beginTime = System.currentTimeMillis();
        for (Map.Entry<String, Long> db : dbSize.entrySet()) {
            String dbKey = db.getKey();
            migrationKeyOneHost(jedis, Integer.valueOf(dbKey.substring("db".length())), keyPre, cluster);
        }

        System.out.println("scanTotalCount->" + scanTotalCount + " migrationCount->" + migrationTotalCount + " useTime->" + ((System.currentTimeMillis() - beginTime) / 1000) + "s");
    }

    static ScanParams sp = new ScanParams();

    static {
        sp.count(10000);
    }

    static long scanTotalCount = 0, migrationTotalCount = 0;


    /**
     * 按key导出数据
     */
    public static void migrationKeyOneHost(Jedis nodeCli, int db, String keyPre, JedisCluster cluster) {
        String[] exportKeyPre = keyPre.split(",");
        nodeCli.select(db);
        long scanCount = 0, migrationCount = 0;
        long beginTime = System.currentTimeMillis();
        String info = nodeCli.info("Keyspace");
        long dbKeySize = 0;
        if (info.indexOf("db0:keys=") > 0) {
            String value = info.substring(info.indexOf("db0:keys=") + "db0:keys=".length()).split(",")[0];
            dbKeySize = Long.valueOf(value);
        }

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
                    System.out.println("thisScanSize:" + thisScanSize + "/" + dbKeySize + " thisExportSize:" + thisExportSize
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
                if ("hash".equals(keyType)) {
                    Map<String, String> value = nodeCli.hgetAll(key);
                    cluster.hmset(clusterKey, value);
                    json.put("value", value);
                } else if ("string".equals(keyType)) {
                    String value = nodeCli.get(key);
                    cluster.set(clusterKey, value);
                    json.put("value", value);
                } else if ("list".equals(keyType)) {
                    int readSize, readCount = 1;
                    long start = 0, end = start + readCount;
                    List<String> value = new ArrayList<String>();
                    do {
                        List<String> data = nodeCli.lrange(key, start, end);
                        readSize = data.size();
                        for (int i = 0; i < readSize; i++) {
                            value.add(data.get(i));
                        }
                        start = end + 1;
                        end += readSize;
                        cluster.rpush(clusterKey, (String[]) data.toArray());
                    } while (readSize == readCount + 1);
                    json.put("value", value);
                } else if ("set".equals(keyType)) {
                    String scursor = "0";
                    List<String> value = new ArrayList<String>();
                    do {
                        ScanResult<String> sscanResult = nodeCli.sscan(key, scursor, sp);
                        scursor = sscanResult.getStringCursor();
                        List<String> tmp = new ArrayList<String>(sscanResult.getResult().size());
                        for (String data : sscanResult.getResult()) {
                            value.add(data);
                            tmp.add(data);
                        }
                        cluster.sadd(clusterKey, tmp.toArray(new String[0]));
                    } while (!"0".equals(scursor));
                    json.put("value", value);
                } else if ("zset".equals(keyType)) {
                    String zcursor = "0";
                    List<Map<String, Double>> value = new ArrayList();
                    do {
                        ScanResult<Tuple> sscanResult = nodeCli.zscan(key, zcursor, sp);
                        zcursor = sscanResult.getStringCursor();
                        Map<String, Double> dataJson = new HashMap<String, Double>();
                        for (Tuple data : sscanResult.getResult()) {
                            dataJson.put(data.getElement(), data.getScore());
                            value.add(dataJson);
                        }
                        cluster.zadd(clusterKey, dataJson);
                    } while (!"0".equals(zcursor));
                    json.put("value", value);
                } else if ("none".equals(keyType)) {//刚好过期的key,可以不用管
                } else {
                    System.out.println("unknow keyType:" + keyType + " key:" + key);
                }
                thisExportSize++;
                migrationCount++;

                writeLog(json);
            }
        } while ((!"0".equals(cursor)));

        System.out.println("scanCount->" + scanCount + " migrationCount->" + migrationCount + " useTime->" + ((System.currentTimeMillis() - beginTime) / 1000) + "s");
        scanTotalCount += scanCount;
        migrationTotalCount += migrationCount;
    }

    static BufferedWriter bw = null;

    private static void writeLog(JSONObject json) {
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
            } else if (arg.startsWith("redisPort=")) {
                redisPort = Integer.valueOf(arg.split("=")[1]);
            } else if (arg.startsWith("clusterPort=")) {
                clusterPort = Integer.valueOf(arg.split("=")[1]);
            } else if (arg.startsWith("logFilePath=")) {
                logFilePath = arg.split("=")[1];
                bw = new BufferedWriter(new FileWriter(logFilePath, true));
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

package tech.huit.redis.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.JedisClusterCRC16;

import java.io.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * redis cluster 管理工具
 *
 * @author huit
 */
public class RedisClusterManager {
    private static String REDIS_HOST = SystemConf.get("REDIS_HOST");
    private static int REDIS_PORT = Integer.parseInt(SystemConf.get("REDIS_PORT"));
    private static JedisCluster cluster;
    static final int DEFAULT_TIMEOUT = 2000;
    static final int MAX_REDIRECTIONS = 25;//应该大于等于主节点数
    static ScanParams sp = new ScanParams();

    static {
        sp.count(1000);
    }

    private static void connectCluser() {
        Set<HostAndPort> nodes = new HashSet<HostAndPort>();
        nodes.add(new HostAndPort(REDIS_HOST, REDIS_PORT));
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(1000);
        poolConfig.setMaxIdle(10);
        poolConfig.setMinIdle(1);
        poolConfig.setMaxWaitMillis(30000);
        //poolConfig.setTestOnBorrow(true);
        //poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        cluster = new JedisCluster(nodes, DEFAULT_TIMEOUT, MAX_REDIRECTIONS, poolConfig);
    }

    public RedisClusterManager() {
        REDIS_HOST = SystemConf.get("REDIS_HOST");
        REDIS_PORT = Integer.valueOf(SystemConf.get("REDIS_PORT"));
    }

    private static AtomicLong writeCount = new AtomicLong();
    private static AtomicLong lastWriteCount = new AtomicLong();
    private static AtomicLong scanCount = new AtomicLong();
    private static AtomicLong lastReadCount = new AtomicLong();
    private static long writeBeginTime = System.currentTimeMillis(), readLastCountTime, writeLastCountTime;
    private static final DecimalFormat speedFormat = new DecimalFormat("#,##0.00");//格式化设置

    private static boolean isCompleted = false;

    /**
     * 按照key前缀查询
     */
    public void importKey(String importKey, final String filePath) {
        final String[] importKeyPre = importKey.split(",");

        final List<JSONObject> dataQueue = Collections.synchronizedList(new LinkedList<JSONObject>());// 待处理数据队列

        final Thread[] writeThread = new Thread[cluster.getClusterNodes().size() * 3];//节点数的3倍
        Thread readThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    BufferedReader br = new BufferedReader(new FileReader(filePath));
                    String data = null;
                    while ((data = br.readLine()) != null) {
                        try {
                            JSONObject json = JSONObject.parseObject(data);
                            dataQueue.add(json);
                        } catch (Exception e) {
                            System.out.println("dataParseError:" + data);
                        }
                        long count = scanCount.incrementAndGet();
                        if (count % 50000 == 0) {
                            if (readLastCountTime > 0) {
                                long useTime = System.currentTimeMillis() - readLastCountTime;
                                float speed = (float) ((count - lastReadCount.get()) / (useTime / 1000.0));
                                System.out.println("read count:" + count + " speed:" + speedFormat.format(speed));
                            }
                            readLastCountTime = System.currentTimeMillis();
                            lastReadCount.set(count);
                            synchronized (dataQueue) {
                                Collections.shuffle(dataQueue);//导出是按节点导出的，这样可以提升性能
                            }
                            while (dataQueue.size() > 100000) {//防止内存写爆了
                                Thread.sleep(1000);
                            }
                        }
                    }
                    br.close();

                    synchronized (dataQueue) {
                        Collections.shuffle(dataQueue);
                    }
                    isCompleted = true;

                    while (!dataQueue.isEmpty()) {//等待数据写入完成
                        Thread.sleep(500);
                    }
                    long useTime = System.currentTimeMillis() - writeBeginTime, totalCount = scanCount.get();
                    float speed = (float) (totalCount / (useTime / 1000.0));
                    System.out.println("write total:" + totalCount + " speed:" + speedFormat.format(speed)
                            + " useTime:" + (useTime / 1000.0) + "s");
                    for (int i = 0; i <= writeThread.length - 1; i++) {
                        writeThread[i].interrupt();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        readThread.start();

        for (int i = 0; i <= writeThread.length - 1; i++) {
            writeThread[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (!isCompleted || !dataQueue.isEmpty()) {
                        JSONObject json = null;
                        if (dataQueue.isEmpty()) {
                            try {
                                Thread.sleep(100);
                                continue;
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        } else {
                            try {
                                synchronized (dataQueue) {
                                    json = dataQueue.remove(0);
                                }
                            } catch (IndexOutOfBoundsException e) {
                                continue;
                            }
                        }
                        String key = json.getString("key");
                        String type = json.getString("type");
                        Object oject = json.get("value");
                        boolean isNeedImport = false;
                        for (String keyImport : importKeyPre) {
                            if ("*".equals(keyImport) || key.startsWith(keyImport)) {
                                isNeedImport = true;
                                break;
                            }
                        }

                        //list使用合并
                        if (isNeedImport) {
                            if ("hash".equals(type)) {
                                JSONArray value = (JSONArray) oject;
                                Iterator<Object> it = value.iterator();
                                Map<String, String> hash = new HashMap<String, String>();
                                while (it.hasNext()) {
                                    JSONObject jsonData = (JSONObject) it.next();
                                    String dataKey = jsonData.getString("key");
                                    String dataValue = jsonData.getString("value");
                                    hash.put(dataKey, dataValue);
                                }
                                cluster.hmset(key, hash);
                            } else if ("string".equals(type)) {
                                String dataValue = (String) oject;
                                cluster.set(key, dataValue);
                            } else if ("list".equals(type)) {
                                JSONArray value = (JSONArray) oject;
                                List<String> inDb = cluster.lrange(key, 0, -1);
                                Iterator<Object> it = value.iterator();
                                while (it.hasNext()) {
                                    String dataValue = (String) it.next();
                                    if (!inDb.contains(dataValue)) {//list使用合并
                                        cluster.rpush(key, dataValue);
                                    } else {
                                        //	System.out.println("value:" + value);
                                    }
                                }
                            } else if ("set".equals(type)) {
                                JSONArray value = (JSONArray) oject;
                                Iterator<Object> it = value.iterator();
                                while (it.hasNext()) {
                                    String dataValue = (String) it.next();
                                    cluster.sadd(key, dataValue);
                                }
                            } else if ("zset".equals(type)) {
                                JSONArray value = (JSONArray) oject;
                                Iterator<Object> it = value.iterator();
                                while (it.hasNext()) {
                                    JSONObject jsonData = (JSONObject) it.next();
                                    double score = jsonData.getLong("score");
                                    String dataValue = jsonData.getString("value");
                                    cluster.zadd(key, score, dataValue);
                                }
                            } else {
                                System.out.println("unknow keyType:" + type + "key:" + key);
                            }
                            long count = writeCount.incrementAndGet();

                            if (count % 10000 == 0) {
                                if (writeLastCountTime > 0) {
                                    long useTime = System.currentTimeMillis() - writeLastCountTime;
                                    float speed = (float) ((count - lastWriteCount.get()) / (useTime / 1000.0));
                                    System.out.println("write count:" + count + "/" + scanCount + " speed:"
                                            + speedFormat.format(speed));
                                }
                                writeLastCountTime = System.currentTimeMillis();
                                lastWriteCount.set(count);
                            }
                        }
                    }
                }
            }, "write thread [" + i + "]");
            writeThread[i].setDaemon(true);
            writeThread[i].start();
        }
    }

    /**
     * 按key导出数据
     */
    public void exportKeyPre(String keyPre, final String filePath) {
        final String[] exportKeyPre = keyPre.split(",");
        createExportFile(filePath + ".0");
        Iterator<Entry<String, JedisPool>> nodes = cluster.getClusterNodes().entrySet().iterator();
        List<Thread> exportTheadList = new ArrayList<Thread>();
        while (nodes.hasNext()) {
            Entry<String, JedisPool> entry = nodes.next();
            try {
                entry.getValue().getResource();
            } catch (redis.clients.jedis.exceptions.JedisConnectionException e) {//有失败的节点连不上
                System.out.println(entry.getKey() + " conn error:" + e.getMessage());
                continue;
            }
            final Jedis nodeCli = entry.getValue().getResource();
            String info = entry.getValue().getResource().info();
            if (info.contains("role:slave")) {//只导出master
                continue;
            }
            Thread exportThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    String cursor = "0";
                    do {
                        ScanResult<String> keys = nodeCli.scan(cursor);
                        cursor = keys.getStringCursor();
                        List<String> result = keys.getResult();
                        for (String key : result) {
                            boolean isExport = false;
                            for (String keyExport : exportKeyPre) {
                                if ("*".equals(keyExport) || key.startsWith(keyExport)) {
                                    isExport = true;
                                    break;
                                }
                            }
                            long count = scanCount.incrementAndGet();
                            if (count % 1000000 == 0) {
                                if (readLastCountTime > 0) {
                                    long useTime = System.currentTimeMillis() - readLastCountTime;
                                    float speed = (float) ((count - lastReadCount.get()) / (useTime / 1000.0));
                                    System.out.println("scan count:" + count + " speed:" + speedFormat.format(speed));
                                }
                                readLastCountTime = System.currentTimeMillis();
                                lastReadCount.set(count);
                            }
                            if (!isExport) {
                                continue;
                            }

                            JSONObject json = new JSONObject();
                            json.put("key", key);
                            String keyType = nodeCli.type(key);
                            json.put("type", keyType);
                            if ("hash".equals(keyType)) {
                                String hcursor = "0";
                                JSONArray value = new JSONArray();
                                do {
                                    ScanResult<Entry<String, String>> hscanResult = nodeCli.hscan(key, hcursor, sp);
                                    hcursor = hscanResult.getStringCursor();
                                    for (Entry<String, String> entry : hscanResult.getResult()) {
                                        JSONObject valueData = new JSONObject();
                                        valueData.put("key", entry.getKey());
                                        valueData.put("value", entry.getValue());
                                        value.add(valueData);
                                    }
                                } while (!"0".equals(hcursor));
                                json.put("value", value);
                            } else if ("string".equals(keyType)) {
                                json.put("value", nodeCli.get(key));
                            } else if ("list".equals(keyType)) {
                                int readSize, readCount = 100;
                                long start = 0, end = start + readCount;
                                JSONArray value = new JSONArray();
                                do {
                                    List<String> data = nodeCli.lrange(key, start, end);
                                    readSize = data.size();
                                    for (int i = 0; i < readSize; i++) {
                                        value.add(data.get(i));
                                    }
                                    start = end + 1;
                                    end += readSize;
                                } while (readSize == readCount + 1);
                                json.put("value", value);
                            } else if ("set".equals(keyType)) {
                                String scursor = "0";
                                JSONArray value = new JSONArray();
                                do {
                                    ScanResult<String> sscanResult = nodeCli.sscan(key, scursor, sp);
                                    scursor = sscanResult.getStringCursor();
                                    for (String data : sscanResult.getResult()) {
                                        value.add(data);
                                    }
                                } while (!"0".equals(scursor));
                                json.put("value", value);
                            } else if ("zset".equals(keyType)) {
                                String zcursor = "0";
                                JSONArray value = new JSONArray();
                                do {
                                    ScanResult<Tuple> sscanResult = nodeCli.zscan(key, zcursor, sp);
                                    zcursor = sscanResult.getStringCursor();
                                    for (Tuple data : sscanResult.getResult()) {
                                        JSONObject dataJson = new JSONObject();
                                        dataJson.put("score", data.getScore());
                                        dataJson.put("value", data.getElement());
                                        value.add(dataJson);
                                    }
                                } while (!"0".equals(zcursor));
                                json.put("value", value);
                            } else {
                                System.out.println("unknow keyType:" + keyType + "key:" + key);
                            }

                            json.put("time", System.currentTimeMillis());
                            writeFile(json.toJSONString(), "export", filePath);
                        }
                    } while (!"0".equals(cursor));
                }
            }, entry.getKey() + "export thread");
            exportTheadList.add(exportThread);
            exportThread.start();
        }

        waitThread(exportTheadList);

        long useTime = System.currentTimeMillis() - writeBeginTime, totalCount = writeCount.get();
        float speed = (float) (totalCount / (useTime / 1000.0));
        System.out.println("scan count:" + scanCount.get() + " export total:" + totalCount + " speed:"
                + speedFormat.format(speed) + " useTime:" + (useTime / 1000.0) + "s");

        try {
            if (null != bw) {
                bw.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static BufferedWriter bw = null;

    public static synchronized void createExportFile(String filePath) {
        if (filePath.lastIndexOf("/") > 0) {
            String pathDir = filePath.substring(0, filePath.lastIndexOf("/"));
            File file = new File(pathDir);
            if (!file.isDirectory()) {
                file.mkdirs();
            }
        }

        File f = new File(filePath);
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(f);
            // write UTF8 BOM mark if file is empty
            if (f.length() < 1) {
                final byte[] bom = new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};
                fos.write(bom);
            }
        } catch (IOException ex) {
        } finally {
            try {
                fos.close();
            } catch (Exception ex) {
            }
        }

        try {
            bw = new BufferedWriter(new FileWriter(filePath, true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static final long FILE_PARTITION_LINE_COUNT = 1000000;//100W

    public static synchronized void writeFile(String data, String optType, String filePath) {
        try {
            if (null == bw) {
                createExportFile(filePath + ".0");
            }
            bw.write(data);
            bw.write('\r');
            bw.write('\n');
            long count = writeCount.incrementAndGet();
            if (count % 100000 == 0) {
                if (writeLastCountTime > 0) {
                    long useTime = System.currentTimeMillis() - writeLastCountTime;
                    float speed = (float) ((count - lastWriteCount.get()) / (useTime / 1000.0));
                    System.out.println(optType + " count:" + count + " speed:" + speedFormat.format(speed));
                }
                writeLastCountTime = System.currentTimeMillis();
                lastWriteCount.set(count);
            }
            if (count % FILE_PARTITION_LINE_COUNT == 0) {//分文件 100W
                bw.close();
                createExportFile(filePath + "." + (count / FILE_PARTITION_LINE_COUNT));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void exportKeysFile(String keyFilePath, String filePath) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(keyFilePath));
            String data = null;
            while ((data = br.readLine()) != null) {
                exportKeys(data, filePath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != br) {
                    br.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * hook线程
     */
    static class CleanWorkThread extends Thread {
        @Override
        public void run() {
            try {
                if (null != bw) {
                    bw.close();
                    System.out.println("bw closed");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 按key导出数据
     */
    public void keySizeCount(String key, String filePath) {
        filePath += key;
        String hcursor = "0";
        JSONObject json = new JSONObject();
        do {
            ScanResult<Tuple> hscanResult = cluster.zscan(key, hcursor, sp);
            hcursor = hscanResult.getStringCursor();
            String fileExt;
            for (Tuple entry : hscanResult.getResult()) {
                String uidKey = entry.getElement();
                long zcard = cluster.zcard("u_f_" + uidKey);
                json.put("uid", uidKey);
                json.put("zcard", zcard);
                if (zcard > 100000) {
                    List<String> nickname = cluster.hmget("rpcUserInfo" + uidKey, "nickname");
                    if (null != nickname && nickname.size() > 0) {
                        json.put("nickname", nickname.get(0));
                    }
                    fileExt = "10W+";
                } else if (zcard > 10000 && zcard <= 100000) {
                    fileExt = "1W-10W";
                } else if (zcard > 1000 && zcard <= 10000) {
                    fileExt = "1k-1W";
                } else if (zcard > 500 && zcard <= 1000) {
                    fileExt = "500-1000";
                } else if (zcard > 300 && zcard <= 500) {
                    fileExt = "300-500";
                } else if (zcard > 200 && zcard <= 300) {
                    fileExt = "200-300";
                } else if (zcard > 100 && zcard <= 200) {
                    fileExt = "100-200";
                } else if (zcard >= 1 && zcard <= 100) {
                    fileExt = "1-100";
                } else {
                    fileExt = "0";
                }

                BufferedWriter bw = null;
                try {
                    bw = new BufferedWriter(new FileWriter(filePath + fileExt, true));
                    bw.write(json.toJSONString());
                    bw.write('\r');
                    bw.write('\n');
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (null != bw) {
                            bw.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                long count = scanCount.incrementAndGet();
                if (count % 10000 == 0) {
                    if (readLastCountTime > 0) {
                        long useTime = System.currentTimeMillis() - readLastCountTime;
                        float speed = (float) ((count - lastReadCount.get()) / (useTime / 1000.0));
                        System.out.println(" count:" + count + " speed:" + speedFormat.format(speed));
                    }
                    readLastCountTime = System.currentTimeMillis();
                    lastReadCount.set(count);
                }
            }
        } while (!"0".equals(hcursor));
    }

    /**
     * 按key导出数据
     */
    public boolean exportKeys(String keys, String filePath) {
        for (String key : keys.split(",")) {
            JSONObject json = new JSONObject();
            json.put("key", key);
            String keyType = cluster.type(key);
            json.put("type", keyType);
            if ("hash".equals(keyType)) {
                String hcursor = "0";
                JSONArray value = new JSONArray();
                do {
                    ScanResult<Entry<String, String>> hscanResult = cluster.hscan(key, hcursor, sp);
                    hcursor = hscanResult.getStringCursor();
                    for (Entry<String, String> entry : hscanResult.getResult()) {
                        JSONObject valueData = new JSONObject();
                        valueData.put("key", entry.getKey());
                        valueData.put("value", entry.getValue());
                        value.add(valueData);
                    }
                } while (!"0".equals(hcursor));
                json.put("value", value);
            } else if ("string".equals(keyType)) {
                json.put("value", cluster.get(key));
            } else if ("list".equals(keyType)) {
                int readSize, readCount = 1;
                long start = 0, end = start + readCount;
                JSONArray value = new JSONArray();
                do {
                    List<String> data = cluster.lrange(key, start, end);
                    readSize = data.size();
                    for (int i = 0; i < readSize; i++) {
                        value.add(data.get(i));
                    }
                    start = end + 1;
                    end += readSize;
                } while (readSize == readCount + 1);
                json.put("value", value);
            } else if ("set".equals(keyType)) {
                String scursor = "0";
                JSONArray value = new JSONArray();
                do {
                    ScanResult<String> sscanResult = cluster.sscan(key, scursor, sp);
                    scursor = sscanResult.getStringCursor();
                    for (String data : sscanResult.getResult()) {
                        value.add(data);
                    }
                } while (!"0".equals(scursor));
                json.put("value", value);
            } else if ("zset".equals(keyType)) {
                String zcursor = "0";
                JSONArray value = new JSONArray();
                do {
                    ScanResult<Tuple> sscanResult = cluster.zscan(key, zcursor, sp);
                    zcursor = sscanResult.getStringCursor();
                    for (Tuple data : sscanResult.getResult()) {
                        JSONObject dataJson = new JSONObject();
                        dataJson.put("score", data.getScore());
                        dataJson.put("value", data.getElement());
                        value.add(dataJson);
                    }
                } while (!"0".equals(zcursor));
                json.put("value", value);
            } else if ("none".equals(keyType)) {//已经被其它人删除
                return true;
            } else {
                String info = "unknowKeyType:" + keyType + "key:" + key;
                System.out.println(info);
                BufferedWriter bw = null;
                try {
                    bw = new BufferedWriter(new FileWriter(filePath + ".error", true));
                    bw.write(info);
                    bw.write('\r');
                    bw.write('\n');
                    return true;
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (null != bw) {
                            bw.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                return false;
            }
            synchronized (this) {//多线程里会调用这个方法
                BufferedWriter bw = null;
                try {
                    bw = new BufferedWriter(new FileWriter(filePath, true));
                    bw.write(json.toJSONString());
                    bw.write('\r');
                    bw.write('\n');
                    return true;
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (null != bw) {
                            bw.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return false;
    }

    /**
     * 按key导出数据
     */
    public void exportHostKeys(String host, String port, String keys, String filePath) {
        String[] keysInfo = keys.split(",");
        Jedis nodeCli = new Jedis(host, Integer.valueOf(port));
        long beginTime = System.currentTimeMillis();
        for (String key : keysInfo) {
            JSONObject json = new JSONObject();
            json.put("key", key);
            String keyType = nodeCli.type(key);
            json.put("type", keyType);
            if ("hash".equals(keyType)) {
                String hcursor = "0";
                JSONArray value = new JSONArray();
                do {
                    ScanResult<Entry<String, String>> hscanResult = nodeCli.hscan(key, hcursor, sp);
                    hcursor = hscanResult.getStringCursor();
                    for (Entry<String, String> entry : hscanResult.getResult()) {
                        JSONObject valueData = new JSONObject();
                        valueData.put("key", entry.getKey());
                        valueData.put("value", entry.getValue());
                        value.add(valueData);
                    }
                } while (!"0".equals(hcursor));
                json.put("value", value);
            } else if ("string".equals(keyType)) {
                json.put("value", nodeCli.get(key));
            } else if ("list".equals(keyType)) {
                int readSize, readCount = 1;
                long start = 0, end = start + readCount;
                JSONArray value = new JSONArray();
                do {
                    List<String> data = nodeCli.lrange(key, start, end);
                    readSize = data.size();
                    for (int i = 0; i < readSize; i++) {
                        value.add(data.get(i));
                        //System.out.println("data:" + data.get(i));
                    }
                    start = end + 1;
                    end += readSize;
                } while (readSize == readCount + 1);
                json.put("value", value);
            } else if ("set".equals(keyType)) {
                String scursor = "0";
                JSONArray value = new JSONArray();
                do {
                    ScanResult<String> sscanResult = nodeCli.sscan(key, scursor, sp);
                    scursor = sscanResult.getStringCursor();
                    for (String data : sscanResult.getResult()) {
                        value.add(data);
                    }
                } while (!"0".equals(scursor));
                json.put("value", value);
            } else if ("zset".equals(keyType)) {
                String zcursor = "0";
                JSONArray value = new JSONArray();
                do {
                    ScanResult<Tuple> sscanResult = nodeCli.zscan(key, zcursor, sp);
                    zcursor = sscanResult.getStringCursor();
                    for (Tuple data : sscanResult.getResult()) {
                        JSONObject dataJson = new JSONObject();
                        dataJson.put("score", data.getScore());
                        dataJson.put("value", data.getElement());
                        value.add(dataJson);
                    }
                } while (!"0".equals(zcursor));
                json.put("value", value);
            } else {
                System.out.println("unknow keyType:" + keyType + "key:" + key);
            }
            BufferedWriter bw = null;
            try {
                bw = new BufferedWriter(new FileWriter(filePath));
                bw.write(json.toJSONString());
                bw.write('\r');
                bw.write('\n');
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (null != bw) {
                        bw.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        nodeCli.close();
        String useTime = " useTime->" + ((System.currentTimeMillis() - beginTime) / 1000) + "s";
        System.out.println(useTime);
    }

    /**
     * 按key导出数据
     */
    public void exportKeyOneHost(String keyPre, String filePath) {
        String[] exportKeyPre = keyPre.split(",");
        Jedis nodeCli = new Jedis(REDIS_HOST, REDIS_PORT);
        long scanTotalcount = 0, exportTotalCount = 0;
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
                scanTotalcount++;
                if (thisScanSize % 1000 == 0) {
                    System.out.println("thisScanSize:" + thisScanSize + "/" + dbKeySize + " thisExportSize:"
                            + thisExportSize + " totalUseTime:" + (System.currentTimeMillis() - beginTime) / 1000
                            + "s)");
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
                String keyType = nodeCli.type(key);
                json.put("type", keyType);
                if ("hash".equals(keyType)) {
                    String hcursor = "0";
                    JSONArray value = new JSONArray();
                    do {
                        ScanResult<Entry<String, String>> hscanResult = nodeCli.hscan(key, hcursor, sp);
                        hcursor = hscanResult.getStringCursor();
                        for (Entry<String, String> entry : hscanResult.getResult()) {
                            JSONObject valueData = new JSONObject();
                            valueData.put("key", entry.getKey());
                            valueData.put("value", entry.getValue());
                            value.add(valueData);
                        }
                    } while (!"0".equals(hcursor));
                    json.put("value", value);
                } else if ("string".equals(keyType)) {
                    json.put("value", nodeCli.get(key));
                } else if ("list".equals(keyType)) {
                    int readSize, readCount = 1;
                    long start = 0, end = start + readCount;
                    JSONArray value = new JSONArray();
                    do {
                        List<String> data = nodeCli.lrange(key, start, end);
                        readSize = data.size();
                        for (int i = 0; i < readSize; i++) {
                            value.add(data.get(i));
                            //System.out.println("data:" + data.get(i));
                        }
                        start = end + 1;
                        end += readSize;
                    } while (readSize == readCount + 1);
                    json.put("value", value);
                } else if ("set".equals(keyType)) {
                    String scursor = "0";
                    JSONArray value = new JSONArray();
                    do {
                        ScanResult<String> sscanResult = nodeCli.sscan(key, scursor, sp);
                        scursor = sscanResult.getStringCursor();
                        for (String data : sscanResult.getResult()) {
                            value.add(data);
                        }
                    } while (!"0".equals(scursor));
                    json.put("value", value);
                } else if ("zset".equals(keyType)) {
                    String zcursor = "0";
                    JSONArray value = new JSONArray();
                    do {
                        ScanResult<Tuple> sscanResult = nodeCli.zscan(key, zcursor, sp);
                        zcursor = sscanResult.getStringCursor();
                        for (Tuple data : sscanResult.getResult()) {
                            JSONObject dataJson = new JSONObject();
                            dataJson.put("score", data.getScore());
                            dataJson.put("value", data.getElement());
                            value.add(dataJson);
                        }
                    } while (!"0".equals(zcursor));
                    json.put("value", value);
                } else {
                    System.out.println("unknow keyType:" + keyType + "key:" + key);
                }
                //						System.out.println("data json:" + json);
                BufferedWriter bw = null;
                try {
                    bw = new BufferedWriter(new FileWriter(filePath, true));
                    bw.write(json.toJSONString());
                    bw.write('\r');
                    bw.write('\n');
                    thisExportSize++;
                    exportTotalCount++;
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (null != bw) {
                            bw.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } while ((!"0".equals(cursor)));

        nodeCli.close();
        SimpleDateFormat dfs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss [");
        String useTime = " useTime->" + ((System.currentTimeMillis() - beginTime) / 1000) + "s";
        System.out.println(dfs.format(new Date()) + "exportKey:" + keyPre + "]" + useTime);
        System.out.println("scanTotalCount->" + scanTotalcount + " exportTotalCount->" + exportTotalCount);
    }

    /**
     * 按照key前缀查询
     */
    public void queryKeyLike(String pattern) {
        Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT);
        String nodes = jedis.clusterNodes();
        jedis.close();
        int count = 0;
        long beginTime = System.currentTimeMillis();
        for (String node : nodes.split("\n")) {
            node = node.replaceAll("@[0-9]*", "");
            String[] nodeInfo = node.split("\\s+");
            String host = nodeInfo[1].split(":")[0];
            int port = Integer.valueOf(nodeInfo[1].split(":")[1]);
            String type = nodeInfo[2];
            if (type.contains("master")) {
                Jedis nodeCli = new Jedis(host, port);//连接redis
                Set<String> keys = nodeCli.keys(pattern);//
                Iterator<String> t1 = keys.iterator();

                while (t1.hasNext()) {
                    String key = t1.next();
                    System.out.println(key + "->" + nodeCli.get(key));
                    count++;
                }
                nodeCli.close();
            }
        }
        SimpleDateFormat dfs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss [");
        System.out.println(dfs.format(new Date()) + pattern + "] query count->" + count + " useTime->"
                + ((System.currentTimeMillis() - beginTime)) + "ms ");
    }

    /**
     * 按照key前缀统计
     */
    public void countKeyLike(String pattern) {
        Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT);
        String nodes = jedis.clusterNodes();
        jedis.close();
        int count = 0;
        long beginTime = System.currentTimeMillis();
        for (String node : nodes.split("\n")) {
            node = node.replaceAll("@[0-9]*", "");
            String[] nodeInfo = node.split("\\s+");
            String host = nodeInfo[1].split(":")[0];
            int port = Integer.valueOf(nodeInfo[1].split(":")[1]);
            String type = nodeInfo[2];
            if (type.contains("master")) {
                Jedis nodeCli = new Jedis(host, port);//连接redis
                Set<String> keys = nodeCli.keys(pattern);//
                count += keys.size();
                nodeCli.close();
            }
        }
        SimpleDateFormat dfs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss [");
        System.out.println(dfs.format(new Date()) + pattern + "] count->" + count + " useTime->"
                + ((System.currentTimeMillis() - beginTime)) + "ms ");
    }

    /**
     * 监控集群状态
     */
    public void monitor(String[] args) {
        double connected_clients = 0, total_commands_processed = 0, instantaneous_ops_per_sec = 0, total_net_input_bytes = 0, total_net_output_bytes = 0, instantaneous_input_kbps = 0, instantaneous_output_kbps = 0, used_memory = 0;
        long keyTotalCount = 0;
        DecimalFormat formatDouble = new DecimalFormat("##0.00");//格式化设置
        DecimalFormat formatLong = new DecimalFormat("##0");//格式化设置
        Map<String, String> opsMap = new TreeMap<String, String>();
        Map<String, String> ramMap = new TreeMap<String, String>();
        Map<String, String> inputMap = new TreeMap<String, String>();
        Map<String, String> outputMap = new TreeMap<String, String>();
        Iterator<Entry<String, JedisPool>> nodes = cluster.getClusterNodes().entrySet().iterator();
        while (nodes.hasNext()) {
            Entry<String, JedisPool> entry = nodes.next();
            JedisPool pool = entry.getValue();
            String info = null;
            Jedis jedis;
            try {
                jedis = pool.getResource();
                info = jedis.info();
                pool.returnResourceObject(jedis);
            } catch (JedisConnectionException e) {
                String msg = e.getMessage();
                if (msg.contains("Connection refused")) {
                    System.out.println(entry.getKey() + " Connection refused");
                    continue;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (info.contains("role:slave")) {//只统计master
                continue;
            }
            connected_clients += getValue(info, "connected_clients");
            total_commands_processed += getValue(info, "total_commands_processed");
            instantaneous_ops_per_sec += getValue(info, "instantaneous_ops_per_sec");
            opsMap.put(entry.getKey(), formatDouble.format(getValue(info, "instantaneous_ops_per_sec")));

            total_net_input_bytes += getValue(info, "total_net_input_bytes");
            total_net_output_bytes += getValue(info, "total_net_output_bytes");

            instantaneous_input_kbps += getValue(info, "instantaneous_input_kbps");
            inputMap.put(entry.getKey(), formatDouble.format(getValue(info, "instantaneous_input_kbps") / 1024) + "KB");

            instantaneous_output_kbps += getValue(info, "instantaneous_output_kbps");
            outputMap.put(entry.getKey(), formatDouble.format(getValue(info, "instantaneous_output_kbps") / 1024)
                    + "KB");

            used_memory += getValue(info, "used_memory");
            ramMap.put(entry.getKey(), formatDouble.format(getValue(info, "used_memory") / 1024 / 1024) + "MB");
            if (info.indexOf("db0:keys=") > 0) {
                String value = info.substring(info.indexOf("db0:keys=") + "db0:keys=".length()).split(",")[0];
                keyTotalCount += Long.valueOf(value);
            }
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        StringBuffer sb = new StringBuffer();
        sb.append(sdf.format(new Date()));
        sb.append(",");
        sb.append(formatLong.format(connected_clients));
        sb.append(",");
        sb.append(formatLong.format(total_commands_processed));
        sb.append(",");
        sb.append(formatLong.format(instantaneous_ops_per_sec));
        sb.append(",");
        sb.append(formatDouble.format(total_net_input_bytes / 1024 / 1024));
        sb.append(",");
        sb.append(formatDouble.format(total_net_output_bytes / 1024 / 1024));
        sb.append(",");
        sb.append(formatDouble.format(instantaneous_input_kbps));
        sb.append(",");
        sb.append(formatDouble.format(instantaneous_output_kbps));
        sb.append(",");
        sb.append(formatDouble.format(used_memory / 1024 / 1024));
        sb.append(",");
        sb.append(keyTotalCount);
        System.out.println(sb.toString());
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(SystemConf.confFileDir + "/monitor.csv", true));
            bw.write(sb.toString());
            bw.write('\r');
            bw.write('\n');
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != bw) {
                    bw.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     */
    public void info(String[] args) {
        double connected_clients = 0, total_commands_processed = 0, instantaneous_ops_per_sec = 0, total_net_input_bytes = 0, total_net_output_bytes = 0, instantaneous_input_kbps = 0, instantaneous_output_kbps = 0, used_memory = 0;
        long keyTotalCount = 0;
        DecimalFormat formatDouble = new DecimalFormat("#,##0.00");//格式化设置
        DecimalFormat formatLong = new DecimalFormat("#,##0");//格式化设置
        Map<String, String> opsMap = new TreeMap<String, String>();
        Map<String, String> ramMap = new TreeMap<String, String>();
        Map<String, String> inputMap = new TreeMap<String, String>();
        Map<String, String> outputMap = new TreeMap<String, String>();
        Iterator<Entry<String, JedisPool>> nodes = cluster.getClusterNodes().entrySet().iterator();
        while (nodes.hasNext()) {
            Entry<String, JedisPool> entry = nodes.next();
            String info = null;
            try {
                info = entry.getValue().getResource().info();
            } catch (JedisConnectionException e) {
                String msg = e.getMessage();
                if (msg.contains("Connection refused")) {
                    System.out.println(entry.getKey() + " Connection refused");
                    continue;
                }
            }
            if (null == info || info.contains("role:slave")) {//只统计master
                continue;
            }
            connected_clients += getValue(info, "connected_clients");
            total_commands_processed += getValue(info, "total_commands_processed");
            instantaneous_ops_per_sec += getValue(info, "instantaneous_ops_per_sec");
            opsMap.put(entry.getKey(), formatDouble.format(getValue(info, "instantaneous_ops_per_sec")));

            total_net_input_bytes += getValue(info, "total_net_input_bytes");
            total_net_output_bytes += getValue(info, "total_net_output_bytes");

            instantaneous_input_kbps += getValue(info, "instantaneous_input_kbps");
            inputMap.put(entry.getKey(), formatDouble.format(getValue(info, "instantaneous_input_kbps") / 1024) + "KB");

            instantaneous_output_kbps += getValue(info, "instantaneous_output_kbps");
            outputMap.put(entry.getKey(), formatDouble.format(getValue(info, "instantaneous_output_kbps") / 1024)
                    + "KB");

            used_memory += getValue(info, "used_memory");
            ramMap.put(entry.getKey(), formatDouble.format(getValue(info, "used_memory") / 1024 / 1024) + "MB");
            if (info.indexOf("db0:keys=") > 0) {
                String value = info.substring(info.indexOf("db0:keys=") + "db0:keys=".length()).split(",")[0];
                keyTotalCount += Long.valueOf(value);
            }
        }

        if (args.length >= 2) {
            Iterator<Entry<String, String>> it;
            for (int i = 0; i < args.length; i++) {
                if ("ops".equals(args[i])) {
                    it = opsMap.entrySet().iterator();
                    System.out.println("instantaneous_ops_per_sec");
                    while (it.hasNext()) {
                        Entry<String, String> entry = it.next();
                        System.out.println(entry.getKey() + "->" + entry.getValue());
                    }
                    System.out.println("instantaneous_ops_per_sec:" + formatLong.format(instantaneous_ops_per_sec));
                } else if ("input".equals(args[i])) {
                    it = inputMap.entrySet().iterator();
                    System.out.println("instantaneous_input_kbps");
                    while (it.hasNext()) {
                        Entry<String, String> entry = it.next();
                        System.out.println(entry.getKey() + "->" + entry.getValue());
                    }
                    System.out.println("total_net_input_bytes:"
                            + formatDouble.format(total_net_input_bytes / 1024 / 1024) + "MB");
                } else if ("output".equals(args[i])) {
                    it = outputMap.entrySet().iterator();
                    System.out.println("instantaneous_output_kbps");
                    while (it.hasNext()) {
                        Entry<String, String> entry = it.next();
                        System.out.println(entry.getKey() + "->" + entry.getValue());
                    }
                    System.out.println("total_net_output_bytes:"
                            + formatDouble.format(total_net_output_bytes / 1024 / 1024) + "MB");
                } else if ("ram".equals(args[i])) {
                    it = ramMap.entrySet().iterator();
                    System.out.println("used_memory");
                    while (it.hasNext()) {
                        Entry<String, String> entry = it.next();
                        System.out.println(entry.getKey() + "->" + entry.getValue());
                    }
                    System.out.println("used_memory:" + formatDouble.format(used_memory / 1024 / 1024) + "MB");
                }
            }
        } else {
            System.out.println("connected_clients:" + formatLong.format(connected_clients));
            System.out.println("total_commands_processed:" + formatLong.format(total_commands_processed));
            System.out.println("instantaneous_ops_per_sec:" + formatLong.format(instantaneous_ops_per_sec));
            System.out.println("total_net_input_bytes:" + formatDouble.format(total_net_input_bytes / 1024 / 1024)
                    + "MB");
            System.out.println("total_net_output_bytes:" + formatDouble.format(total_net_output_bytes / 1024 / 1024)
                    + "MB");
            System.out.println("instantaneous_input_kbps:" + formatDouble.format(instantaneous_input_kbps));
            System.out.println("instantaneous_output_kbps:" + formatDouble.format(instantaneous_output_kbps));
            System.out.println("used_memory:" + formatDouble.format(used_memory / 1024 / 1024) + "MB");
            System.out.println("keyTotalCount:" + keyTotalCount);
        }
    }

    private double getValue(String info, String key) {
        String value;
        value = info.substring(info.indexOf(key) + key.length() + 1).split("\r\n")[0];
        return Double.valueOf(value);
    }

    /**
     * 查询key
     */
    public void keys(String pattern) {
        Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT);
        String nodes = jedis.clusterNodes();
        jedis.close();
        int count = 0;
        long beginTime = System.currentTimeMillis();
        for (String node : nodes.split("\n")) {
            node = node.replaceAll("@[0-9]*", "");
            String[] nodeInfo = node.split("\\s+");
            String host = nodeInfo[1].split(":")[0];
            int port = Integer.valueOf(nodeInfo[1].split(":")[1]);
            String type = nodeInfo[2];
            if (type.contains("master")) {
                Jedis nodeCli = new Jedis(host, port);//连接redis
                Set<String> keys = nodeCli.keys(pattern);
                System.out.println("host:" + host + " port:" + port);
                for (String key : keys) {
                    System.out.println(key);
                }
                nodeCli.close();
            }
        }
        SimpleDateFormat dfs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss [");
        System.out.println(dfs.format(new Date()) + pattern + "*] count->" + count + " useTime->"
                + ((System.currentTimeMillis() - beginTime)) + "ms ");
    }

    /**
     * 统计所有的key数量
     */
    public long keySize() {
        Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT);
        String nodes = jedis.clusterNodes();
        jedis.close();
        long count = 0;
        long beginTime = System.currentTimeMillis();
        for (String node : nodes.split("\n")) {
            node = node.replaceAll("@[0-9]*", "");
            String[] nodeInfo = node.split("\\s+");
            String host = nodeInfo[1].split(":")[0];
            int port = Integer.valueOf(nodeInfo[1].split(":")[1]);
            String type = nodeInfo[2];
            if (type.contains("master") && !type.contains("fail")) {
                try {
                    Jedis nodeCli = new Jedis(host, port);//连接redis

                    String info = nodeCli.info("Keyspace");
                    if (info.indexOf("db0:keys=") > 0) {
                        String value = info.substring(info.indexOf("db0:keys=") + "db0:keys=".length()).split(",")[0];
                        count += Long.valueOf(value);
                    }
                    nodeCli.close();
                } catch (Exception e) {
                }
            }
        }
        System.out.println("clusterKeySize:" + count + " useTime->" + ((System.currentTimeMillis() - beginTime))
                + "ms ");
        return count;
    }

    /**
     * 按照key前缀清除缓存
     */
    public void dels(String keyPre, final String filePath) {
        final String[] exportKeyPre = keyPre.split(",");
        createExportFile(filePath);
        Iterator<Entry<String, JedisPool>> nodes = cluster.getClusterNodes().entrySet().iterator();
        List<Thread> exportTheadList = new ArrayList<Thread>();
        while (nodes.hasNext()) {
            Entry<String, JedisPool> entry = nodes.next();
            try {
                entry.getValue().getResource();
            } catch (redis.clients.jedis.exceptions.JedisConnectionException e) {//有失败的节点连不上
                System.out.println(entry.getKey() + " conn error:" + e.getMessage());
                continue;
            }
            final Jedis nodeCli = entry.getValue().getResource();
            String info = entry.getValue().getResource().info();
            if (info.contains("role:slave")) {//只能从master删除
                continue;
            }
            Thread exportThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    String cursor = "0";
                    do {
                        ScanResult<String> keys = nodeCli.scan(cursor, sp);
                        cursor = keys.getStringCursor();
                        List<String> result = keys.getResult();
                        for (String key : result) {
                            scanCount.incrementAndGet();
                            for (String keyExport : exportKeyPre) {
                                if ("*".equals(keyExport) || key.startsWith(keyExport)) {
                                    nodeCli.del(key);
                                    writeFile(key, "del", filePath);
                                    break;
                                }
                            }
                        }
                    } while ((!"0".equals(cursor)));
                }
            }, entry.getKey() + "-del thread");
            exportTheadList.add(exportThread);
            exportThread.start();
        }

        waitThread(exportTheadList);

        long useTime = System.currentTimeMillis() - writeBeginTime, totalCount = scanCount.get();
        float speed = (float) (totalCount / (useTime / 1000.0));
        System.out.println("scanCount:" + scanCount.get() + " delTotal:" + writeCount.get()
                + " speed:" + speedFormat.format(speed) + " useTime:" + (useTime / 1000.0) + "s");

        try {
            if (null != bw) {
                bw.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * java -jar redis-cluster-util-jar-with-dependencies.jar h
     */
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        //		args = new String[] { "add-master", "172.20.16.87:29000", "172.20.16.88:29000", "172.20.16.89:29000" };
        //		args = new String[] { "add-master", "172.20.16.87:29000", "172.20.16.88:29000" };
        //args = new String[] { "add-master", "172.20.16.87:29005" };
        //		args = new String[] { "analyze", "isKeyStat=true", "isCmdDetail=true", "showTop=20", "host=172.20.16.48",
        //				"port=5001", "maxCountLine=5" };
        //args = new String[] { "add-slave","172.20.16.87:29000->172.20.16.88:29000;172.20.16.87:29001->172.20.16.88:29001" };
        //args = new String[] { "add-slave","172.20.16.87:29001->172.20.16.88:29001" };
        //args = new String[] { "add-node", "172.20.16.91:29010", "172.20.16.89:29010" };
        //args = new String[] { "bakup-node", "D://abc" };
        //		args = new String[] { "benchmark", "E:/bakup/jumei-app/show-dev-data-export.dat", "10" };
        //		args = new String[] { "check" };
        //		args = new String[] { "count" };
//        args = new String[]{"create", "10.0.6.200:6000->;10.0.6.200:6001->;10.0.6.200:6002->;10.0.6.200:6003->;10.0.6.200:6004->;10.0.6.200:6005->;10.0.6.200:6006->;10.0.6.200:6007->;"};
        //		args = new String[] { "del" };
        //		args = new String[] { "dels" };
        //		args = new String[] { "del-node", ":0" };
        //				args = new String[] { "del-node", "172.20.16.87:29000" };
        //		args = new String[] { "del-node", "172.20.16.88:29000;172.20.16.89:29000" };
        //args = new String[] { "export", "*", "d:/show-dev-data-export.dat" };
        //args = new String[] { "export-keys", "s_f_p_9186_86964530,s_f_p_7580_68233821", "d:/show-key-export.dat" };
        //		args = new String[] { "export-keys-file", "d:/keys.txt", "d:/show-key-export.dat" };
        //args = new String[] { "fix-slot", "172.20.16.88:29000" };
        //		args = new String[] { "failover", "192.168.254.130:5001" };
        //		args = new String[] { "fix-slot-cover", "192.168.254.129:5001" };
        //		args = new String[] { "fix-slot-stable", "192.168.254.129:5001" };
        //		args = new String[] { "flush" };
        //		args = new String[] { "get" };
        //		args = new String[] { "import", "l,s", "d:/show-dev-data-export.dat" };
        //args = new String[] { "import", "*", "E:/bakup/jumei-app/show-online-2016.2.3.dat" };
        //		args = new String[] { "import-mongodb", "*", "D:/bakup/jumeiapp-redis/show-imported-list.2016.1.11.dat" };
        //		args = new String[] { "info" };
        //		args = new String[] { "info", "output", "ops" };
//        args = new String[]{"keys", "*"};
        //		args = new String[] { "keysize"};
        //args = new String[] { "monitor", "2" };
        //		args = new String[] { "raminfo", "*" };
        //args = new String[] { "raminfo", "172.20.16.89:5001" };
        //args = new String[] { "rubbish-del" };
        //args = new String[] { "key-size-count", "u_id_set", "D:/" };
        //args = new String[] { "reshard", "172.20.16.87:29000", "0-1024;1025-2048;4096-4096;4098-4301" };
        //"reshard"  "192.168.254.129:5000"  "0-1024;1025-2048;4096-4096;4098-4301"
        //		args = new String[] { "set", "testkey", "testvalue" };
        //				args = new String[] { "h" };
        //args = new String[] { "followRestore", "*", "D:/29000-u_f.dat" };
        //args = "safe-delete safe-delete 1".split(" ");
        Runtime.getRuntime().addShutdownHook(new CleanWorkThread());
        RedisClusterManager rcm = new RedisClusterManager();
        long beginTime = System.currentTimeMillis();
        if (args.length == 0) {
            printHelp();
            return;
        }

        String cmd = args[0];
        if ("raminfo".equals(cmd) || "exporth".equals(cmd) || "exportHostKeys".equals(cmd)) {
        } else {
            connectCluser();
        }

        if (args.length > 0) {
            if ("add-slave".equals(cmd)) {
                if (args.length == 2) {
                    String[] master2slave = trim(args[1]).split(";");
                    for (int i = 0; i < master2slave.length; i++) {
                        String[] hostsInfo = master2slave[i].split("->");
                        if (hostsInfo.length == 2) {
                            rcm.addSlave(hostsInfo[0], hostsInfo[1], false);
                        } else {
                            System.out.println("请输入要添加的节点及主节点列表");
                        }
                    }
                    Thread.sleep(3000);//等待集群配置同步
                    rcm.check();
                } else {
                    System.out.println("请输入主备关系：host1:port1->host2:port1;host1:port2->host2:port2;");
                }
            } else if ("bakup-node".equals(cmd)) {
                if (args.length == 2) {
                    rcm.bakupNode(args[1]);
                } else {
                    System.out.println("参数错误！");
                }
            } else if ("analyze".equals(cmd)) {
                MonitorUtil.main(args);
            } else if ("exportHostKeys".equals(cmd)) {
                if (args.length == 5) {
                    rcm.exportHostKeys(args[1], args[2], args[3], args[4]);
                } else {
                    System.out.println("exportHostKeys ip port key1,key2 D:/export.dat");
                }
            } else if ("raminfo".equals(cmd)) {
                if (args.length == 2) {
                    rcm.raminfo(args[1]);
                } else {
                    connectCluser();
                    rcm.raminfo(null);
                }
            } else if ("create".equals(cmd)) {
                StringBuffer sb = new StringBuffer();
                for (int i = 1; i < args.length; i++) {
                    sb.append(args[i]);
                }
                String hostTrim = trim(sb.toString());

                String[] master2slave = hostTrim.split(";");
                rcm.create(rcm, master2slave);
                Thread.sleep(3000);//等待集群配置同步
                rcm.check();
            } else if ("reshard".equals(cmd)) {
                rcm.reshard(args);
            } else if ("failover".equals(cmd)) {
                String[] slaves = trim(args[1]).split(";");
                for (String slave : slaves) {
                    rcm.failOver(slave);
                }
                Thread.sleep(3000);//等待集群配置同步
                rcm.check();
            } else if ("fix-slot-cover".equals(cmd)) {
                rcm.fixSlotCover(args[1]);
                Thread.sleep(3000);//等待集群配置同步
                rcm.check();
            } else if ("fix-slot-stable".equals(cmd)) {
                rcm.fixSlotStable();
                Thread.sleep(3000);//等待集群配置同步
                rcm.check();
            } else if ("add-master".equals(cmd)) {
                if (args.length >= 2) {
                    rcm.addMaster(args);
                    Thread.sleep(3000);//等待集群配置同步
                    rcm.check();
                } else {
                    System.out.println("请输入要添加的 主节点");
                }
            } else if ("dels".equals(cmd)) {
                if (args.length == 3) {
                    rcm.dels(args[1], args[2]);
                } else {
                    System.out.println("dels keyPattern D:/delKey.dat");
                }
            } else if ("counts".equals(cmd)) {
                if (args.length == 1) {
                    System.out.println("请输入要统计的key前缀");
                } else {
                    for (int i = 1; i < args.length; i++) {
                        rcm.countKeyLike(args[i]);
                    }
                }
            } else if ("del-node".equals(cmd)) {
                if (args.length == 2) {
                    String[] hostsInfo = trim(args[1]).split(";");
                    for (int i = 0; i < hostsInfo.length; i++) {
                        rcm.delNode(hostsInfo[i]);
                    }
                    Thread.sleep(3000);//等待集群配置同步
                    rcm.check();
                } else {
                    System.out.println("请输入要删除的节点:host1:port1;host2:port2;");
                }
            } else if ("querys".equals(cmd)) {
                if (args.length == 1) {
                    rcm.queryKeyLike("");
                } else {
                    for (int i = 1; i < args.length; i++) {
                        rcm.queryKeyLike(args[i]);
                    }
                }
            } else if ("export".equals(cmd)) {
                if (args.length == 3) {
                    rcm.exportKeyPre(args[1], args[2]);
                } else {
                    System.out.println("export keyPattern D:/export.dat");
                }
            } else if ("exporth".equals(cmd)) {
                if (args.length == 3) {
                    rcm.exportKeyOneHost(args[1], args[2]);
                } else {
                    System.out.println("export keyPattern D:/export.dat");
                }
            } else if ("export-keys".equals(cmd)) {
                if (args.length == 3) {
                    rcm.exportKeys(args[1], args[2]);
                } else {
                    System.out.println("export keys D:/export.dat");
                }
            } else if ("export-keys-file".equals(cmd)) {
                if (args.length == 3) {
                    rcm.exportKeysFile(args[1], args[2]);
                } else {
                    System.out.println("export keys D:/export.dat");
                }
            } else if ("import".equals(cmd)) {
                if (args.length == 3) {
                    rcm.importKey(args[1], args[2]);
                } else {
                    System.out.println("import keyPattern D:/import.dat");
                }
            } else if ("import-mongodb".equals(cmd)) {
                if (args.length == 3) {
                    //rcm.importMongodb(args[1], args[2]);
                } else {
                    System.out.println("import keyPattern D:/import.dat");
                }
            } else if ("restoreShowPraise".equals(cmd)) {
                if (args.length == 2) {
                    rcm.restoreShowPraise(args[1]);
                } else {
                    System.out.println("restoreUserHash uid1,uid2");
                }
            } else if ("followRestoreByUids".equals(cmd)) {
                if (args.length == 2) {
                    rcm.followRestoreByUids(args[1]);
                } else {
                    System.out.println("followRestoreByUids uid1,uid2");
                }
            } else if ("set".equals(cmd) || "del".equals(cmd)) {
                rcm.opt(args);
            } else if ("get".equals(cmd)) {
                rcm.opt(args);
            } else if (("" +
                    "").equals(cmd)) {
                if (args.length == 1) {
                    System.out.println("请输入要查詢的key前缀");
                } else {
                    rcm.keys(args[1]);
                }
            } else if ("keysize".equals(cmd)) {
                rcm.keySize();
            } else if ("key-size-count".equals(cmd)) {
                if (args.length == 3) {
                    rcm.keySizeCount(args[1], args[2]);
                } else {
                    System.out.println("key-size-count u_id_set D:/");
                }
            } else if ("info".equals(cmd)) {
                rcm.info(args);
            } else if ("monitor".equals(cmd)) {
                long sleepTime = 1000;
                if (args.length == 2) {
                    sleepTime = Long.valueOf(args[1]) * 1000;
                }
                while (true) {
                    try {
                        rcm.monitor(args);
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                    Thread.sleep(sleepTime);
                }
            } else if ("check".equals(cmd)) {
                rcm.check();
            } else if ("flush".equals(cmd)) {
                rcm.flushall();
            } else if ("h".equals(cmd) || "-h".equals(cmd) || "help".equals(cmd)) {
                printHelp();
            } else {
                Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT);
                Map<Integer, String> slot2Host = new HashMap<Integer, String>();
                List<Object> slotInfos = jedis.clusterSlots();
                for (Object slotInfo : slotInfos) {
                    List<Object> slotInfoList = (List<Object>) slotInfo;
                    long begin = (Long) slotInfoList.get(0);
                    long end = (Long) slotInfoList.get(1);
                    @SuppressWarnings("rawtypes")
                    List hostInfoList = (ArrayList) slotInfoList.get(2);
                    String host = new String((byte[]) hostInfoList.get(0));
                    int port = Integer.valueOf(hostInfoList.get(1).toString());
                    String hostInfo = host + ":" + port;
                    for (int i = (int) begin; i <= end; i++) {
                        slot2Host.put(i, hostInfo);
                    }
                }
                jedis.close();

                String key = args[1];
                int slot = JedisClusterCRC16.getCRC16(key) % 16384;
                String[] hostInfo = null;
                String hostPort = slot2Host.get(slot);
                if (null != hostPort) {
                    hostInfo = hostPort.split(":");
                    cmd = "redis-cli -h " + hostInfo[0] + " -p " + hostInfo[1];
                    for (int i = 0; i < args.length; i++) {
                        cmd = cmd + " " + args[i];
                    }
                    executeCmd(cmd);
                } else {
                    System.out.println("not cover solt:" + slot);
                }
            }
            for (String arg : args) {
                System.out.print(arg + " ");
            }
            System.out.println("finish use time " + ((System.currentTimeMillis() - beginTime)) + "ms");
        }
    }

    public Map<String, AtomicLong> ramSizeCount = new ConcurrentHashMap<String, AtomicLong>();
    public Map<String, AtomicLong> ramKeyCount = new ConcurrentHashMap<String, AtomicLong>();
    public StringBuffer ramUnknowKey = new StringBuffer();

    private void writeRamInfo() {
        BufferedWriter raminfoUnknow = null;
        try {
            Iterator<Entry<String, AtomicLong>> it = ramKeyCount.entrySet().iterator();
            System.out.println("key type size:" + ramKeyCount.size());
            bw = new BufferedWriter(new FileWriter(SystemConf.confFileDir + "/raminfo.csv"));
            bw = new BufferedWriter(new FileWriter(SystemConf.confFileDir + "/raminfo.csv"));
            while (it.hasNext()) {
                Entry<String, AtomicLong> entry = it.next();
                String info = entry.getKey() + "," + entry.getValue() + "," + ramSizeCount.get(entry.getKey()) + "\r\n";
                bw.write(info);
            }
            raminfoUnknow = new BufferedWriter(new FileWriter(SystemConf.confFileDir + "/raminfoUnknowKey.txt", true));
            ramUnknowKey.append("\r\n");
            raminfoUnknow.write(ramUnknowKey.toString());
            ramUnknowKey = new StringBuffer();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != bw) {
                    bw.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                if (null != raminfoUnknow) {
                    raminfoUnknow.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 按key分类进行统计
     */
    public void raminfo(String node) {
        List<Thread> exportTheadList = new ArrayList<Thread>();
        if (null != node) {
            String[] hostInfo = node.split(":");
            Jedis jedis = new Jedis(hostInfo[0], Integer.valueOf(hostInfo[1]));
            nodeAnalyze(exportTheadList, node, jedis);
        } else {
            Iterator<Entry<String, JedisPool>> nodes = cluster.getClusterNodes().entrySet().iterator();
            while (nodes.hasNext()) {
                Entry<String, JedisPool> entry = nodes.next();
                if (null != node) {
                    if (!node.equals(entry.getKey())) {
                        continue;
                    }
                }
                final Jedis nodeCli = entry.getValue().getResource();
                String info = entry.getValue().getResource().info();
                if (null == node && info.contains("role:slave")) {//如果没有指定节点，统计所有master
                    continue;
                }
                nodeAnalyze(exportTheadList, entry.getKey(), nodeCli);
            }
        }

        waitThread(exportTheadList);

        writeRamInfo();

        long useTime = System.currentTimeMillis() - writeBeginTime, totalCount = scanCount.get();
        float speed = (float) (totalCount / (useTime / 1000.0));
        System.out.println("scan total:" + totalCount + " speed:" + speedFormat.format(speed) + " useTime:"
                + (useTime / 1000.0) + "s");
    }

    private void nodeAnalyze(List<Thread> exportTheadList, String node, final Jedis nodeCli) {
        Thread exportThread = new Thread(new Runnable() {
            @Override
            public void run() {
                String cursor = "0";
                int len = "serializedlength:".length();
                do {
                    ScanResult<String> keys = nodeCli.scan(cursor, sp);
                    cursor = keys.getStringCursor();
                    List<String> result = keys.getResult();
                    for (String key : result) {
                        String debug = nodeCli.debug(DebugParams.OBJECT(key));
                        int startIndex = debug.indexOf("serializedlength:");
                        int endIndex = debug.indexOf(" ", startIndex);
                        debug = debug.substring(startIndex + len, endIndex);

                        int i = 0;
                        //key = "s_c_p23926";//testkey
                        //key = "26228273praiseto101909365showid10290";//testkey

                        if (key.startsWith("rpcUserInfo")) {
                            key = "rpcUserInfo";
                        } else if (key.startsWith("s_url")) {
                            key = "s_url";
                        } else if (key.startsWith("live_link_")) {
                            key = "live_link_";
                        } else if (key.startsWith("historyappmessages")) {
                            key = "historyappmessages";
                        } else if (key.startsWith("historyadminmessages")) {
                            key = "historyadminmessages";
                        } else if (key.contains("praiseto") && key.contains("showid")) {
                            key = "praisetoshowid";
                        } else if (key.contains("followuser")) {
                            key = "followuser";
                        } else if (key.startsWith("user_relations")) {
                            key = "user_relations";
                        } else if (key.startsWith("user_relation_")) {
                            key = "user_relation_";
                        } else {
                            char c;
                            boolean isFindDecollator = false, isKnowBusiness = false;
                            for (; i < key.length(); i++) {
                                c = key.charAt(i);
                                if (key.charAt(i) == '_') {
                                    isFindDecollator = true;
                                }
                                if (c == ':') {
                                    isFindDecollator = true;
                                    key = key.substring(0, i);
                                    break;
                                } else if (isFindDecollator && i > 0 && c >= '0' && c <= '9') {
                                    key = key.substring(0, i);
                                    isKnowBusiness = true;
                                    break;
                                }
                            }
                            if (!isKnowBusiness && !isFindDecollator) {//没有加业务前缀
                                ramUnknowKey.append(key).append(',');
                                key = "unknown";
                            }
                        }

                        AtomicLong sizeCount = ramSizeCount.get(key);
                        if (null == sizeCount) {
                            sizeCount = new AtomicLong();
                            ramSizeCount.put(key, sizeCount);
                        }
                        sizeCount.addAndGet(Long.valueOf(debug));

                        AtomicLong keyCount = ramKeyCount.get(key);
                        if (null == keyCount) {
                            keyCount = new AtomicLong();
                            ramKeyCount.put(key, keyCount);
                        }
                        keyCount.incrementAndGet();
                        scanCount.incrementAndGet();
                        if (scanCount.get() % 100000 == 0) {
                            System.out.print("scan key size:" + scanCount.get());
                            writeRamInfo();
                        }
                    }
                } while ((!"0".equals(cursor)));
            }

        }, node + "-raminfo");
        exportTheadList.add(exportThread);
        exportThread.start();
    }

    static void waitThread(List<Thread> delThreadList) {
        for (Thread thread : delThreadList) {
            do {
                if (thread.isAlive()) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } while (thread.isAlive());
        }
    }

    private void bakupNode(String filePath) {
        Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT);
        String nodes = jedis.clusterNodes();
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(filePath));
            for (String node : nodes.split("\n")) {
                node = node.replaceAll("@[0-9]*", "");
                bw.write(node);
                bw.write("\\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != bw) {
                    bw.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static String trim(String sb) {
        String hostTrim = sb.toString().replace(" ", "");
        hostTrim = hostTrim.replace("\r", "");
        hostTrim = hostTrim.replace("\n", "");
        hostTrim = hostTrim.replace("\\", "");
        return hostTrim;
    }

    //"reshard", "172.20.162.87:29000", "0-1024;1025-2048"
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void reshard(String[] args) {
        Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT);
        Jedis destinationNode = connect(args[1]);
        String[] destinationHostInfo = args[1].split(":");
        String destinationHost = destinationHostInfo[0];
        int destinationPort = Integer.valueOf(destinationHostInfo[1]);
        String nodes = jedis.clusterNodes();
        Map<String, String> host2NodeId = new HashMap<String, String>();
        String destination_node_id = null;
        List<Jedis> clusterHostList = new ArrayList<Jedis>();
        for (String node : nodes.split("\n")) {
            node = node.replaceAll("@[0-9]*", "");
            String[] nodeInfo = node.split("\\s+");
            String nodeId = nodeInfo[0];
            String host = nodeInfo[1];
            String type = nodeInfo[2];
            String[] hostInfo = nodeInfo[1].split(":");
            clusterHostList.add(new Jedis(hostInfo[0], Integer.parseInt(hostInfo[1])));
            if (args[1].equals(host)) {
                destination_node_id = nodeId;
                if (type.contains("master")) {
                    destination_node_id = nodeId;
                } else {
                    System.out.println(args[1] + " is not master !");
                    jedis.close();
                    return;
                }
            }
            if (type.contains("master")) {
                host2NodeId.put(host, nodeId);
            }
        }
        if (null == destination_node_id) {
            System.out.println(args[1] + " destination_node_id not found");
            jedis.close();
            return;
        }

        byte[] coverSlot = new byte[16384];
        Map<Integer, Jedis> slot2Host = new HashMap<Integer, Jedis>();
        Map<Integer, String> slot2NodeId = new HashMap<Integer, String>();
        Map<String, Jedis> host2Jedis = new HashMap<String, Jedis>();

        List<Object> slotInfos = jedis.clusterSlots();
        for (Object slotInfo : slotInfos) {
            List<Object> slotInfoList = (List<Object>) slotInfo;
            long begin = (Long) slotInfoList.get(0);
            long end = (Long) slotInfoList.get(1);
            List hostInfoList = (ArrayList) slotInfoList.get(2);
            String host = new String((byte[]) hostInfoList.get(0));
            int port = Integer.valueOf(hostInfoList.get(1).toString());
            String hostInfo = host + ":" + port;
            for (int i = (int) begin; i <= end; i++) {
                coverSlot[i] = 1;
                Jedis jedisHost = host2Jedis.get(hostInfo);
                if (null == jedisHost) {
                    jedisHost = new Jedis(host, port);
                    host2Jedis.put(hostInfo, jedisHost);
                }
                slot2Host.put(i, jedisHost);
                slot2NodeId.put(i, host2NodeId.get(hostInfo));
            }
        }

        String[] slots2Migrating = args[2].split(";");
        int slotBegin = 0, slotEnd = 0;
        int timeout = 15000, migratCount = 10;
        for (String slotRange : slots2Migrating) {
            String[] slotInfo = slotRange.split("-");
            slotBegin = Integer.valueOf(slotInfo[0]);
            if (slotInfo.length == 1) {
                slotEnd = slotBegin;
            } else if (slotInfo.length == 2) {
                slotEnd = Integer.valueOf(slotInfo[1]);
            } else {
                System.out.println("参数错误！");
                jedis.close();
                return;
            }
            System.out.println("migrate slot " + slotRange + " ...");
            for (int slot = slotBegin; slot <= slotEnd; slot++) {
                Jedis sourceNode = slot2Host.get(slot);
                String source_node_id = slot2NodeId.get(slot);
                if (null == source_node_id) {
                    System.out.println(slot + " source_node_id not found");
                    continue;
                }
                if (source_node_id.equals(destination_node_id)) {//同一主机
                    continue;
                }
                destinationNode.clusterSetSlotImporting(slot, source_node_id);//step 1 必需在第二步前
                sourceNode.clusterSetSlotMigrating(slot, destination_node_id);//step 2

                List<String> keysInSlot;
                do {
                    keysInSlot = sourceNode.clusterGetKeysInSlot(slot, migratCount);
                    for (String key : keysInSlot) {
                        try {
                            sourceNode.migrate(destinationHost, destinationPort, key, 0, timeout);////step 3
                        } catch (RuntimeException e) {
                            String msg = e.getMessage();
                            e.printStackTrace();
                            if (msg.contains("BUSYKEY Target key name already exists")) {
                                System.out.println(key + " BUSYKEY Target key name already exists");
                                continue;
                            }
                            System.out.println("迁移终止,当前slot:" + slot + " key:" + key);
                            return;
                        }
                    }
                } while (keysInSlot.size() != 0);

                try {
                    //如果目标节点变成从节点会存在slot丢失
                    String checkNodes = destinationNode.clusterNodes();
                    if (!checkNodes.contains("myself,master")) {
                        System.out.println("目标节点不是主节点，迁移终止，当前slot位置:" + slot);
                        return;
                    }
                    sourceNode.clusterSetSlotNode(slot, destination_node_id);//step 4 source or destination
                    destinationNode.clusterSetSlotNode(slot, destination_node_id);//
                } catch (redis.clients.jedis.exceptions.JedisDataException e) {
                    String msg = e.getMessage();
                    if (msg.contains("I still hold keys")) {
                        slot--; //高写入的情况下可能写入了新数据，否则数据会发生丢失
                        System.out.println(slot + ",I still hold keys,try again");
                        continue;
                    } else {
                        e.printStackTrace();
                        System.out.println("有节点失败，迁移终止，当前slot位置:" + slot);
                        return;
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    System.out.println("有节点失败，迁移终止，当前slot位置:" + slot);
                    return;
                }

                for (Jedis notify : clusterHostList) {
                    try {
                        notify.clusterSetSlotNode(slot, destination_node_id);
                    } catch (Throwable e) {
                        e.printStackTrace();
                        System.out.println("有节点失败，迁移终止，当前slot位置:" + slot);
                        return;
                    }
                }

                //必须强一致性，否则正在迁移的两个节点有失败，solt信息来不及同步会存在丢失slot的情况
                for (Jedis notify : clusterHostList) {
                    int waitCount = 0;
                    boolean isSync = false;
                    String nodeCheck = null;
                    do {
                        try {
                            nodeCheck = notify.clusterInfo();
                        } catch (redis.clients.jedis.exceptions.JedisConnectionException e) {
                            e.printStackTrace();
                            System.out.println("有节点失败，迁移终止，当前slot位置:" + slot);
                        }
                        isSync = nodeCheck.contains("cluster_slots_ok:16384");
                        if (!isSync) {
                            waitCount++;
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            System.out.println("wait conf sync " + waitCount + " ...");
                        }
                    } while (!isSync);
                }

                if (slot % 1 == 0) {//5000W个key的迁移速度还是比较慢的
                    System.out.println("migrate slot " + slot + " done");
                }
            }
            System.out.println("migrate slot " + slotRange + " done");
        }

        destinationNode.close();
        jedis.close();
    }

    private void create(RedisClusterManager rcm, String[] master2slave) {
        String[] masterHost = master2slave[0].split("->");
        String[] hostInfo = masterHost[0].split(":");
        String host = hostInfo[0];
        int port = Integer.parseInt(hostInfo[1]);

        //meet
        for (int i = 0; i < master2slave.length; i++) {
            String[] hostsInfo = master2slave[i].split("->");
            Jedis clusterNode = connect(hostsInfo[0]);
            clusterNode.clusterMeet(host, port);
            clusterNode.close();
            if (hostsInfo.length == 2) {
                Jedis slaveNode = connect(hostsInfo[1]);
                try {
                    slaveNode.clusterMeet(host, port);
                    slaveNode.close();
                } catch (redis.clients.jedis.exceptions.JedisConnectionException e) {
                    System.out.println(hostsInfo[1] + " clusterMeet connect error!");
                }
            } else {
                System.out.println(hostsInfo[0] + "，未添加从节点，存在风险");
            }
        }
        System.out.println("cluster send meet all!");

        //set slot
        int slot = 16384 / master2slave.length;
        int slotIndex = 0;
        for (int i = 0; i < master2slave.length; i++) {
            String[] hostsInfo = master2slave[i].split("->");
            Jedis clusterNode = connect(hostsInfo[0]);
            int thisBegin = slotIndex;
            for (; slotIndex <= (i + 1) * slot && slotIndex < 16384; slotIndex++) {
                try {
                    clusterNode.clusterAddSlots(slotIndex);
                } catch (redis.clients.jedis.exceptions.JedisDataException e) {
                    String msg = e.getMessage();
                    if (msg.contains("is already busy")) {
                    } else {
                        System.out.println("clusterAddSlotsError hostsInfo:" + hostsInfo[0] + " slotIndex:" + slotIndex);
                        e.printStackTrace();
                    }
                } catch (redis.clients.jedis.exceptions.JedisConnectionException e2) {
                    System.out.println(hostsInfo[0] + " clusterAddSlots connect error!");
                }
            }
            if (i == master2slave.length - 1) {//最后一个节点进行slot补全
                for (; slotIndex < 16384; slotIndex++) {
                    try {
                        clusterNode.clusterAddSlots(slotIndex);
                    } catch (redis.clients.jedis.exceptions.JedisDataException e) {
                        String msg = e.getMessage();
                        if (msg.contains("is already busy")) {
                        } else {
                            e.printStackTrace();
                        }
                    } catch (redis.clients.jedis.exceptions.JedisConnectionException e2) {
                        System.out.println(hostsInfo[0] + " clusterAddSlots connect error!");
                    }
                }
            }
            System.out.println(hostsInfo[0] + " set slots " + thisBegin + "-" + (slotIndex - 1));
            clusterNode.close();
        }

        //set slave
        for (int i = 0; i < master2slave.length; i++) {
            String[] hostsInfo = master2slave[i].split("->");
            if (hostsInfo.length == 2) {
                rcm.addSlave(hostsInfo[0], hostsInfo[1], true);
            }
        }
    }

    private Jedis connect(String hostPort) {
        String[] hostInfo = hostPort.split(":");
        Jedis jedis = new Jedis(hostInfo[0], Integer.parseInt(hostInfo[1]));
        try {
            jedis.info();//test
        } catch (Exception e) {
            System.out.println("connect error,host:" + hostInfo[0] + " port:" + hostInfo[1]);
        }
        return jedis;
    }

    private void failOver(String slaveNode) throws Exception {
        String[] masterHostInfo = slaveNode.split(":");
        Jedis fixNode = new Jedis(masterHostInfo[0], Integer.parseInt(masterHostInfo[1]));
        try {
            String clusterNode;
            int tryCount = 0;
            do {
                fixNode.clusterFailover();//不是100%起作用
                Thread.sleep(500);
                tryCount++;
                clusterNode = fixNode.clusterNodes();
                if (tryCount > 1) {
                    System.out.println(slaveNode + " tryCount:" + tryCount);
                }
            } while (clusterNode.contains("myself,slave"));//保证踢成功
            System.out.println(slaveNode + " failover success!");
        } catch (redis.clients.jedis.exceptions.JedisDataException e) {
            String msg = e.getMessage();
            if (msg.contains("CLUSTER FAILOVER to a slave")) {
                System.out.println(slaveNode + " is master, You should send CLUSTER FAILOVER to a slave");
            }
        } catch (redis.clients.jedis.exceptions.JedisConnectionException e2) {
            String msg = e2.getMessage();
            if (msg.contains("connect timed out")) {
                System.out.println(slaveNode + " : connect timed out");
            }
        }
        fixNode.close();
    }

    private void fixSlotStable() {
        Jedis fixNode = new Jedis(REDIS_HOST, REDIS_PORT);
        byte[] coverSlot = new byte[16384];
        List<Object> slotInfos = fixNode.clusterSlots();
        fixNode.close();
        for (Object slotInfo : slotInfos) {//检查删除节点是否含有slot
            @SuppressWarnings("unchecked")
            List<Object> slotInfoList = (List<Object>) slotInfo;
            long begin = (Long) slotInfoList.get(0);
            long end = (Long) slotInfoList.get(1);
            for (int i = (int) begin; i <= end; i++) {
                coverSlot[i] = 1;
                fixNode.clusterSetSlotStable(i);//Clear any importing / migrating state from hash slot.
            }
        }
    }

    /**
     * 使用指定主机修复没有cover的slot
     *
     * @param masterNode
     */
    @SuppressWarnings("unchecked")
    private void fixSlotCover(String masterNode) {
        String[] masterHostInfo = masterNode.split(":");
        Jedis fixNode = new Jedis(masterHostInfo[0], Integer.parseInt(masterHostInfo[1]));

        byte[] coverSlot = new byte[16384];
        List<Object> slotInfos = fixNode.clusterSlots();
        for (Object slotInfo : slotInfos) {//检查删除节点是否含有slot
            List<Object> slotInfoList = (List<Object>) slotInfo;
            long begin = (Long) slotInfoList.get(0);
            long end = (Long) slotInfoList.get(1);
            //			String host = new String((byte[]) hostInfo.get(0));
            //			long port = (long) hostInfo.get(1);

            for (int i = (int) begin; i <= end; i++) {
                coverSlot[i] = 1;
                //fixNode.clusterSetSlotStable(i);//Clear any importing / migrating state from hash slot.
            }
        }
        int begin = -1;
        for (int i = 0; i < 16384; i++) {
            if (coverSlot[i] == 0) {
                fixNode.clusterAddSlots(i);
            }
            if (coverSlot[i] == 0 && begin == -1) {
                begin = i;
            } else if ((coverSlot[i] == 1 && begin > -1) || i == 16384) {
                System.out.println("cluster_slots_fixed:" + begin + "-" + i);
                begin = -1;
            }
        }
        fixNode.close();
    }

    private static void printHelp() {
        System.out.println("java -jar redis-cluster-util-jar-with-dependencies.jar arg1 arg2 ...");
        System.out.println("add-master \t:[host:port;host2:port2]add master list");
        System.out.println("add-slave \t:[maser->slave;master2->slave2;...]master->slave");
        System.out.println("analyze \t:" + MonitorUtil.helpInfo);
        System.out.println("bakup-node \t:[file path]file path to save");
        System.out
                .println("benchmark  \t:java -cp redis-cluster-util-jar-with-dependencies.jar com.jumei.util.Benchmark key value offset limit threadCount [all|set|get]");
        System.out.println("check \t:check cluster status");
        System.out.println("count \t:[keyPattern] count key count use keyPattern");
        System.out.println("create \t:[maser->slave;master2->slave2;...] create cluster");
        System.out.println("del \t:[key] del one key");
        System.out.println("dels \t:[keyPattern][delKeyFileSavePath] del use keyPattern");
        System.out.println("del-node \t:[host:port]");
        System.out.println("del-node-id \t:[node-id]del node use id");
        System.out.println("export \t:[keyPattern][outputFilePath] use * to export all");
        System.out.println("exporth \t:[keyPattern][outputFilePath] export one host data, use * to export all");
        System.out.println("export-keys \t:[key1,key2][outputFilePath]");
        System.out.println("export-keys-file \t:[input keys file][outputFilePath]");
        System.out.println("failover \t:[slaveHost:slavePort;slaveHos2:slavePort2] slave failover");
        System.out.println("fix-slot-cover \t:[host:port] use one node to fix uncovered slot ");
        System.out.println("fix-slot-stable \t:clear any importing / migrating state from hash slot");
        System.out.println("flush \t:use flushall to clean cluster all data (be careful!)");
        System.out.println("get \t:[key] get a string type value");
        System.out
                .println("import \t:[keyPattern][importFilePath] import if key not contains but list use mrege, use * to import all");
        System.out.println("info \t:(ops,input,output,ram) query db info ");
        System.out.println("keys \t:query use keyPattern");
        System.out.println("keysize :count cluster all key");
        System.out.println("monitor :[sleep second] monitor cluster status");
        System.out.println("querys \t:query use pattern");
        System.out.println("reshard \t:[host:port](master) [1-1024;1025-2048](slot range)");
        System.out.println("raminfo \t:[host:port]default all node raminfo analysis");
        System.out.println("set \t:[key][value] set a string type value");
        System.out.println("safe-delete \t:[keyPre][delCount]");
        System.out.println("others \t:use redis-cli to execute others command(linux only)");
    }

    /**
     * 执行shell
     *
     * @param cmd
     */
    public static void executeCmd(String cmd) {
        if (null != cmd) {
            System.out.println("exec cmd: " + cmd);
            if (!SystemConf.isWindos) {
                Runtime rt = Runtime.getRuntime();
                try {
                    long beginTime = System.currentTimeMillis();

                    Process process = rt.exec(cmd);
                    StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR");
                    StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream(), "INFO");
                    errorGobbler.start();
                    outputGobbler.start();
                    System.out.println(cmd + " useTime:" + (System.currentTimeMillis() - beginTime));

                    while (errorGobbler.isAlive() || outputGobbler.isAlive()) {
                        Thread.sleep(1);
                    }
                    process.waitFor();
                    process.destroy();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void flushall() {
        Iterator<Entry<String, JedisPool>> nodes = cluster.getClusterNodes().entrySet().iterator();
        while (nodes.hasNext()) {
            Entry<String, JedisPool> entry = nodes.next();
            Jedis jedis = entry.getValue().getResource();
            try {
                jedis.flushAll();
                System.out.println(entry.getKey() + " flushAll success");
            } catch (Exception e) {
                String msg = e.getMessage();
                if (msg.contains("Read timed out")) {
                    System.out.println(entry.getKey() + " flushAll fail");
                } else if (msg.contains("READONLY")) {//slave
                } else {
                    e.printStackTrace();
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void check() {
        Jedis clusterMaster = new Jedis(REDIS_HOST, REDIS_PORT, 10000);
        String nodes = clusterMaster.clusterNodes();
        Map<String, String> slave2host = new TreeMap<String, String>();
        Map<String, String> host2slave = new TreeMap<String, String>();
        Map<String, String> master2host = new TreeMap<String, String>();
        Map<String, String> host2master = new TreeMap<String, String>();
        Map<String, String> master2slave = new TreeMap<String, String>();
        for (String node : nodes.split("\n")) {
            node = node.replaceAll("@[0-9]*", "");
            String[] nodeInfo = node.split("\\s+");
            String type = nodeInfo[2];
            if (type.contains("master")) {
                master2host.put(nodeInfo[0], nodeInfo[1]);
                host2master.put(nodeInfo[1], nodeInfo[0]);
                master2slave.put(nodeInfo[1], "warn");
            }
        }

        for (String node : nodes.split("\n")) {
            node = node.replaceAll("@[0-9]*", "");
            String[] nodeInfo = node.split("\\s+");
            String type = nodeInfo[2];
            if (type.contains("slave")) {
                slave2host.put(nodeInfo[0], nodeInfo[1]);
                host2slave.put(nodeInfo[1], nodeInfo[0]);
                String masterHost = master2host.get(nodeInfo[3]);
                if (null != masterHost) {
                    master2slave.put(masterHost, nodeInfo[1]);
                } else {
                    System.out.println("master not found:" + nodeInfo[1]);
                }
            }
        }

        Iterator<Entry<String, String>> it = master2slave.entrySet().iterator();
        StringBuffer slaveCheck = new StringBuffer("==== slave status check info ====");
        boolean slaveCheckErrorFind = false;
        while (it.hasNext()) {
            Entry<String, String> entry = it.next();
            String key = entry.getKey();
            String value = entry.getValue();
            if ("warn".equals(value)) {
                slaveCheckErrorFind = true;
                slaveCheck.append("\r\n" + entry.getKey() + " no slave");
                continue;
            }
            String[] masterHostInfo = key.split(":");
            String[] slaveHostInfo = value.split(":");

            if (masterHostInfo[0].equals(slaveHostInfo[0]) || !masterHostInfo[1].equals(slaveHostInfo[1])) {//同一主机或端口不一致
                slaveCheck.append("\r\n" + entry.getKey() + " slave ");
                if (":0".equals(value)) {
                    slaveCheck.append("disconnected");
                } else {
                    slaveCheck.append(value + " warn");
                }
                slaveCheckErrorFind = true;
            } else {
                slaveCheck.append("\r\n" + entry.getKey() + "->" + value);
            }
        }
        if (slaveCheckErrorFind) {
            slaveCheck.insert("==== slave status check info ====".length(), "error");
        } else {
            slaveCheck.insert("==== slave status check info ====".length(), "ok");
        }
        System.out.println(slaveCheck);

        StringBuffer nodeFailCheck = new StringBuffer("==== node status check info ====");
        boolean failCheckFind = false;
        for (String node : nodes.split("\n")) {
            node = node.replaceAll("@[0-9]*", "");
            if (node.contains("fail") || node.contains(":0")) {
                nodeFailCheck.append("\r\n" + node);
                failCheckFind = true;
            }
        }
        if (!failCheckFind) {
            nodeFailCheck.append("ok");
        }
        System.out.println(nodeFailCheck);

        String clusterInf = clusterMaster.clusterInfo();
        if (clusterInf.contains("cluster_state:ok") && clusterInf.contains("cluster_slots_ok:16384")) {
            System.out.println("==== cluster info ====OK");
        } else {
            System.out.println("==== cluster info ====");
            List<Object> slotInfos = clusterMaster.clusterSlots();
            byte[] coverSlot = new byte[16384];
            for (Object slotInfo : slotInfos) {//检查删除节点是否含有slot
                List<Object> slotInfoList = (List<Object>) slotInfo;
                long begin = (Long) slotInfoList.get(0);
                long end = (Long) slotInfoList.get(1);
                for (int i = (int) begin; i <= end; i++) {
                    coverSlot[i] = 1;
                }
            }
            int begin = -1;
            for (int i = 0; i < 16384; i++) {
                /*if (coverSlot[i] == 0) {
                    System.out.println("cluster_slots_lost:" + i);
				}*/
                if (coverSlot[i] == 0 && begin == -1) {
                    if (i == 16383 || coverSlot[i + 1] == 1) {//只丢失了一个slot
                        System.out.println("cluster_slots_lost:" + i);
                    } else {
                        begin = i;
                    }
                } else if ((coverSlot[i] == 1 && begin > -1)) {
                    System.out.println("cluster_slots_lost_range:" + begin + "-" + i);
                    begin = -1;
                }
            }
            System.out.println(clusterInf);
        }

        clusterMaster.close();
    }

    private void addMaster(String[] args) {
        Jedis clusterNode = new Jedis(REDIS_HOST, REDIS_PORT);
        List<Jedis> addHostList = new ArrayList<Jedis>();
        String nodes = null;
        String[] addMasterNodes = trim(args[1]).split(";");
        for (String addMasterNode : addMasterNodes) {
            String[] addHostInfo = addMasterNode.split(":");
            Jedis addNode = new Jedis(addHostInfo[0], Integer.parseInt(addHostInfo[1]));
            try {
                nodes = addNode.clusterNodes();
                addHostList.add(addNode);
            } catch (redis.clients.jedis.exceptions.JedisConnectionException e) {
                System.out.println(addMasterNode + " connect error!");
                continue;
            }
            int nodeCount = 0;
            String addNodeId = null;
            for (String node : nodes.split("\n")) {
                node = node.replaceAll("@[0-9]*", "");
                String[] nodeInfo = node.split("\\s+");
                if (node.contains("myself")) {
                    addNodeId = nodeInfo[0];
                }
                nodeCount++;
            }
            if (null == addNodeId) {
                System.out.println("nodeId not found!");
                return;
            }

            if (nodeCount > 1) {
                System.out.println(addMasterNode + " this is not new node,use cmd to remove old node info");
                System.out.println("cd /home/redis/" + addHostInfo[1]
                        + " && rm -f dump.rdb appendonly.aof nodes.conf redis.log && service redis-node-"
                        + addHostInfo[1] + " restart");
                return;
            }
        }
        for (Jedis addHost : addHostList) {
            boolean meetSeccuss = false;
            addHost.clusterMeet(REDIS_HOST, REDIS_PORT);
            while (!meetSeccuss) {
                try {
                    Thread.sleep(100);//估计需要100ms
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                nodes = clusterNode.clusterNodes();
                if (nodes.contains(getJedisHostInfo(addHost))) {//从cluster里检查是否包含本信息
                    meetSeccuss = true;
                }
                if (!meetSeccuss) {
                    System.out.println(getJedisHostInfo(addHost) + " wait meet to seccuss ...");
                } else {
                    System.out.println(getJedisHostInfo(addHost) + " add master seccussed!");
                }
            }
        }
        clusterNode.close();
    }

    private void addSlave(String masterNode, String slaveNode, boolean isCreateCluster) {
        String[] masterHostInfo = masterNode.split(":");
        Jedis master = new Jedis(masterHostInfo[0], Integer.parseInt(masterHostInfo[1]));
        String nodes = master.clusterNodes();
        String masterNodeId = null;
        List<Jedis> clusterHostList = new ArrayList<Jedis>();
        for (String node : nodes.split("\n")) {
            node = node.replaceAll("@[0-9]*", "");
            if (node.contains("master") && node.contains("connected")) {//只通知master节点
                String[] nodeInfo = node.split("\\s+");
                String hostPort = nodeInfo[1];
                String[] hostInfo = hostPort.split(":");
                if (masterNode.equals(hostPort)) {
                    masterNodeId = nodeInfo[0];
                }
                int port = Integer.parseInt(hostInfo[1]);
                if (port > 0) {
                    clusterHostList.add(new Jedis(hostInfo[0], port));
                } else {
                    //System.out.println("not connected:" + node);//可能存在没有连上的节点
                }
            }
        }

        String[] addHostInfo = slaveNode.split(":");
        Jedis slave = new Jedis(addHostInfo[0], Integer.parseInt(addHostInfo[1]));
        nodes = slave.clusterNodes();
        int nodeCount = 0;
        String addNodeId = null;
        for (String node : nodes.split("\n")) {
            node = node.replaceAll("@[0-9]*", "");
            String[] nodeInfo = node.split("\\s+");
            if (node.contains("myself")) {
                addNodeId = nodeInfo[0];
            }
            nodeCount++;
        }
        if (null == addNodeId) {
            System.out.println("nodeId not found");
            slave.close();
            master.close();
            return;
        }
        if (nodeCount > 1 && !isCreateCluster) {
            System.out.println(slaveNode + " this is not new node,use cmd to remove old node info");
            System.out.println("cd /home/redis/" + addHostInfo[1]
                    + " && rm -f dump.rdb appendonly.aof nodes.conf redis.log && service redis-node-" + addHostInfo[1]
                    + " restart");
            slave.close();
            master.close();
            return;
        }
        if (null == masterNodeId) {
            System.out.println("not found master node with host:" + masterNode);
            slave.close();
            master.close();
            return;
        }

        slave.clusterMeet(masterHostInfo[0], Integer.parseInt(masterHostInfo[1]));
        boolean meetSeccuss = false;
        while (!meetSeccuss) {
            nodes = slave.clusterNodes();
            if (nodes.contains(masterNodeId)) {
                meetSeccuss = true;
            }
            if (!meetSeccuss) {
                System.out.println(masterNode + " wait slave meet success ...");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        try {
            slave.clusterReplicate(masterNodeId);
        } catch (redis.clients.jedis.exceptions.JedisDataException e) {
            String msg = e.getMessage();
            String print = "only replicate a master, not a slave";
            if (msg.contains(print)) {
                System.out.println(masterNode + " " + print);
            } else {
                e.printStackTrace();
            }
        }

        //check
        for (Jedis host : clusterHostList) {
            boolean isAddSuccess = false;
            do {
                String checkNodes = null;
                try {
                    checkNodes = host.clusterNodes();
                } catch (redis.clients.jedis.exceptions.JedisConnectionException e) {
                    System.out.println(getJedisHostInfo(host) + " check slave connect error");
                    continue;
                }
                for (String node : checkNodes.split("\n")) {
                    node = node.replaceAll("@[0-9]*", "");
                    String[] nodeInfo = node.split("\\s+");
                    String hostPort = nodeInfo[1];
                    if (slaveNode.equals(hostPort)) {
                        isAddSuccess = true;
                        break;
                    }
                }
                if (!isAddSuccess) {
                    System.out.println(getJedisHostInfo(host) + " wait nodes.conf sync ...");
                    try {
                        Thread.sleep(300);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } while (!isAddSuccess);
        }
        master.close();
        slave.close();
    }

    /**
     * 删除从节点或没有slot的主节点，失败的节点可以删除
     *
     * @param delNode
     */
    @SuppressWarnings("unchecked")
    private void delNode(String delNode) {
        Jedis checkMaster = new Jedis(REDIS_HOST, REDIS_PORT);
        String clusterNodes = checkMaster.clusterNodes();
        if (!clusterNodes.contains(delNode)) {
            checkMaster.close();
            System.out.println(delNode + " not in cluster!");
            return;
        }
        if (!":0".equals(delNode)) {//掉线主机直接删除 ，TODO有bug
            List<Object> slotInfos = checkMaster.clusterSlots();
            for (Object slotInfo : slotInfos) {//检查删除节点是否含有slot
                List<Object> slotInfoList = (List<Object>) slotInfo;
                for (int i = 2; i < slotInfoList.size(); i++) {
                    List<Object> slotHostInfo = (List<Object>) slotInfoList.get(i);
                    String host = new String((byte[]) slotHostInfo.get(0));
                    long port = (Long) slotHostInfo.get(1);
                    String hostPort = host + ":" + port;
                    String isMasterCheck = hostPort + " master";

                    if ((hostPort.equals(delNode) && clusterNodes.contains(isMasterCheck))) {//master有slot不能删除
                        System.out.println(hostPort + " del fail contain slot " + slotInfoList.get(0) + "-"
                                + slotInfoList.get(1));
                        checkMaster.close();
                        return;
                    }
                }
            }
        }

        List<String> delNodeIds = new ArrayList<String>();//:0 如果不在线是这种格式可能存在多个主机
        List<Jedis> clusterHostList = new ArrayList<Jedis>();
        for (String node : clusterNodes.split("\n")) {
            node = node.replaceAll("@[0-9]*", "");
            String[] nodeInfo = node.split("\\s+");
            String[] hostInfo = nodeInfo[1].split(":");
            if (delNode.equals(nodeInfo[1])) {
                delNodeIds.add(nodeInfo[0]);
            } else {
                clusterHostList.add(new Jedis(hostInfo[0], Integer.parseInt(hostInfo[1])));
            }
        }

        if (delNodeIds.size() > 0) {
            for (String delNodeId : delNodeIds) {
                for (Jedis host : clusterHostList) {
                    String hostInfo = getJedisHostInfo(host);
                    try {
                        host.clusterForget(delNodeId);
                        System.out.println(hostInfo + " send forget sucess");
                    } catch (redis.clients.jedis.exceptions.JedisDataException e) {
                        String msg = e.getMessage();
                        if (null != msg && msg.contains("Unknown node")) {
                            System.out.println(hostInfo + " not found");
                        } else {
                            System.out.println(hostInfo + " send forget fail");
                            e.printStackTrace();
                        }
                    } catch (redis.clients.jedis.exceptions.JedisConnectionException e) {
                        String msg = e.getMessage();
                        if (null != msg && msg.contains("Connection refused")) {
                            System.out.println(hostInfo + " 主机连不上，请手动清空除该节点对应node配置，否则当前主机重新加入集群进会带入被踢出的节点信息!");
                        } else {
                            e.printStackTrace();
                        }
                    }
                }
                //check
                for (Jedis host : clusterHostList) {
                    boolean isDelSuccess = false;
                    while (!isDelSuccess) {
                        String checkNodes = checkMaster.clusterNodes();
                        if (checkNodes.contains(delNodeId)) {
                            System.out.println(getJedisHostInfo(host) + " wait delete success ...");
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        } else {
                            isDelSuccess = true;
                        }
                    }
                }
                String[] delHostInfo = delNode.split(":");
                int port = Integer.parseInt(delHostInfo[1]);
                if (port > 0) {
                    try {
                        Jedis jedis = new Jedis(delHostInfo[0], Integer.parseInt(delHostInfo[1]));
                        jedis.shutdown();
                        System.out.println(delNode + " has shutdown!");
                        jedis.close();
                    } catch (redis.clients.jedis.exceptions.JedisConnectionException e) {
                        System.out.println(delNode + "，无法连接，请手动进行关闭!");
                    }
                }
                System.out.println(delNode + " delete success, please remove nodes.conf file!");
            }
        }

        checkMaster.close();
    }

    private String getJedisHostInfo(Jedis host) {
        return host.getClient().getHost() + ":" + host.getClient().getPort();
    }

    private void opt(String[] args) {
        JedisCluster jedisCluster;
        Set<HostAndPort> jedisClusterNodes;
        JedisPoolConfig pool;
        jedisClusterNodes = new HashSet<HostAndPort>();
        jedisClusterNodes.add(new HostAndPort(REDIS_HOST, REDIS_PORT));

        pool = new JedisPoolConfig();
        pool.setMaxTotal(100);
        jedisCluster = new JedisCluster(jedisClusterNodes, pool);
        long beginTime = System.currentTimeMillis();
        if ("del".equals(args[0])) {
            for (int i = 1; i < args.length; i++) {
                jedisCluster.del(args[i]);
            }
        } else if ("set".equals(args[0])) {
            jedisCluster.set(args[1], args[2]);
        } else if ("get".equals(args[0])) {
            for (int i = 1; i < args.length; i++) {
                System.out.println(args[i] + "->" + jedisCluster.get(args[i]));
            }
        }
        System.out.println("opt useTime->" + ((System.currentTimeMillis() - beginTime)) + "ms ");
        try {
            jedisCluster.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Jedis getOldRedis(String key) {
        return oldRedisMap.get(JedisClusterCRC16.getCRC16(key) % 16384);
    }

    /**
     * 恢复关注队列和粉丝队列
     */
    private void followRestoreByUids(String uids) {
        String[] uidsArray = uids.split(",");
        String KEY_USER_ATTENTION_ZSET = "u_a_";
        String KEY_USER_FANS_ZSET = "u_f_";
        for (String uid : uidsArray) {
            if (uid.length() == 0) {
                continue;//无效的uid
            }
            try {
                //恢复关注
                String keyAttention = KEY_USER_ATTENTION_ZSET + uid;//此用户的关注队列
                int slot = JedisClusterCRC16.getCRC16(keyAttention) % 16384;
                Jedis oldJedis = oldRedisMap.get(slot);
                Set<Tuple> attentionUids = oldJedis.zrangeWithScores(keyAttention, 0, -1);
                if (attentionUids != null && attentionUids.size() > 0) {
                    for (Tuple t : attentionUids) {
                        String attentionUid = t.getElement(); //被关注人的uid
                        double score = t.getScore();
                        cluster.zadd(keyAttention, score, attentionUid);
                        cluster.zadd(KEY_USER_FANS_ZSET + attentionUid, score, uid);//加入被关注人的粉丝队列
                    }
                }

                //恢复粉丝
                String keyFans = KEY_USER_FANS_ZSET + uid;//此用户的粉丝队列
                int slot2222 = JedisClusterCRC16.getCRC16(keyFans) % 16384;
                Jedis oldJedis2222 = oldRedisMap.get(slot2222);
                Set<Tuple> fansUids = oldJedis2222.zrangeWithScores(keyFans, 0, -1);
                if (fansUids != null && fansUids.size() > 0) {
                    for (Tuple t : fansUids) {
                        String fansUid = t.getElement(); //粉丝的uid
                        double score = t.getScore();
                        cluster.zadd(keyFans, score, fansUid);
                        cluster.zadd(KEY_USER_ATTENTION_ZSET + fansUid, score, uid);//加入粉丝的关注队列
                    }
                }
            } catch (Exception e) {
                System.out.println("followRestoreByUids 异常，当前uid:" + uid);
                e.printStackTrace();
            }
        }
    }

    /**
     * 恢复帖子点赞
     */
    private void restoreShowPraise(String uids) {
        String[] uidsArray = uids.split(",");
        String KEY_USER_SHOW_ZSET = "u_s_";//普通帖
        String KEY_USER_SHOW_VIDEO_ZSET = "u_s_v_";//视频帖
        String KEY_USER_COUNSEL_SHOW_ZSET = "u_counsel_s_";//专栏帖
        String KEY_SHOW_PRAISE_SET = "s_p_";//帖子点赞人队列
        for (String uid : uidsArray) {
            if (uid.length() == 0) {//无效的uid
                continue;
            }
            try {
                String key = KEY_USER_SHOW_ZSET + uid;
                //遍历普通帖
                Set<String> showIdsNormal = oldRedisMap.get(JedisClusterCRC16.getCRC16(key) % 16384).zrange(key, 0, -1);
                if (showIdsNormal != null && showIdsNormal.size() > 0) {
                    for (String showId : showIdsNormal) {
                        Set<Tuple> tuplesPraise = getOldRedis(KEY_SHOW_PRAISE_SET + showId).zrangeWithScores(
                                KEY_SHOW_PRAISE_SET + showId, 0, -1);
                        if (tuplesPraise != null && tuplesPraise.size() > 0) {
                            for (Tuple t : tuplesPraise) {
                                String praiseUserId = t.getElement();
                                double praiseTime = t.getScore();
                                cluster.zadd(KEY_SHOW_PRAISE_SET + showId, praiseTime, praiseUserId);
                            }
                        }
                    }
                }

                //遍历视频帖
                Set<String> showIdsVideo = getOldRedis(KEY_USER_SHOW_VIDEO_ZSET + uid).zrange(
                        KEY_USER_SHOW_VIDEO_ZSET + uid, 0, -1);
                if (showIdsVideo != null && showIdsVideo.size() > 0) {
                    for (String showId : showIdsVideo) {
                        Set<Tuple> tuplesPraise = getOldRedis(KEY_SHOW_PRAISE_SET + showId).zrangeWithScores(
                                KEY_SHOW_PRAISE_SET + showId, 0, -1);
                        if (tuplesPraise != null && tuplesPraise.size() > 0) {
                            for (Tuple t : tuplesPraise) {
                                String praiseUserId = t.getElement();
                                double praiseTime = t.getScore();
                                cluster.zadd(KEY_SHOW_PRAISE_SET + showId, praiseTime, praiseUserId);
                            }
                        }
                    }
                }

                //遍历专栏帖
                Set<String> showIdsCounsel = getOldRedis(KEY_USER_COUNSEL_SHOW_ZSET + uid).zrange(
                        KEY_USER_COUNSEL_SHOW_ZSET + uid, 0, -1);
                if (showIdsCounsel != null && showIdsCounsel.size() > 0) {
                    for (String showId : showIdsCounsel) {
                        Set<Tuple> tuplesPraise = getOldRedis(KEY_SHOW_PRAISE_SET + showId).zrangeWithScores(
                                KEY_SHOW_PRAISE_SET + showId, 0, -1);
                        if (tuplesPraise != null && tuplesPraise.size() > 0) {
                            for (Tuple t : tuplesPraise) {
                                String praiseUserId = t.getElement();
                                double praiseTime = t.getScore();
                                cluster.zadd(KEY_SHOW_PRAISE_SET + showId, praiseTime, praiseUserId);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("程序出现异常！当前uid:" + uid + ",异常信息：" + e.getMessage());
            }
        }
    }

    static Map<Integer, String> oldRedisSlot2Host = new HashMap<Integer, String>();
    static Map<Integer, Jedis> oldRedisMap = new HashMap<Integer, Jedis>();

    static {
        String host = null;
        Integer port = null;
        try {
            File file = new File(SystemConf.confFileDir + "/oldRedisSlot2Host.txt");
            if (file.isFile()) {
                BufferedReader br = new BufferedReader(new FileReader(file));
                String data;
                //10.0.228.31:29006#5243-5897
                while ((data = br.readLine()) != null) {
                    String[] info = data.split(":");
                    if (info.length == 3) {
                        host = info[0];
                        port = Integer.valueOf(info[1]);
                        Jedis jedis = new Jedis(host, port);
                        jedis.info();//测试一下是否可通知

                        String[] soltInfo = info[2].split("-");
                        int begin = Integer.valueOf(soltInfo[0]);
                        int end = Integer.valueOf(soltInfo[1]);
                        for (int i = begin; i <= end; i++) {
                            oldRedisMap.put(i, jedis);
                        }
                    }
                }
                br.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("nodeError host:" + host + " port:" + port);
        }
    }
}

package tech.huit.redis.util;

import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.*;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * stage
 * java -cp redis-cluster-manager-jar-with-dependencies.jar ZsetExport host=10.16.32.62 port=29440 key=u_f_93679013 exportType=csv,json
 * <p>
 * prod
 * java -cp redis-cluster-manager-jar-with-dependencies.jar ZsetExport host=10.17.22.4 port=29000 key=u_f_97005914 exportType=csv
 * java -cp redis-cluster-manager-jar-with-dependencies.jar ZsetExport host=10.17.22.4 port=29000 key=u_f_79247828 exportType=csv
 * java -cp redis-cluster-manager-jar-with-dependencies.jar ZsetExport host=10.17.22.4 port=29000 key=u_f_89564771 exportType=csv
 * java -cp redis-cluster-manager-jar-with-dependencies.jar ZsetExport host=10.17.22.4 port=29000 key=u_f_93679013 exportType=csv
 *
 * @author huit
 */
public class ZsetExport {
    private static int port;
    private static String host, key, exportType = "csv";

    private static String helpInfo = "host=172.20.16.48 port=5001 key=93679013 exportType=csv,json";
    private static JedisCluster cluster;

    static ScanParams sp = new ScanParams();

    static {
        sp.count(5000);
    }

    private static void connectCluser() {
        Set<HostAndPort> nodes = new HashSet<HostAndPort>();
        nodes.add(new HostAndPort(host, port));
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(1000);
        poolConfig.setMaxIdle(10);
        poolConfig.setMinIdle(1);
        poolConfig.setMaxWaitMillis(30000);
        poolConfig.setTestWhileIdle(true);
        cluster = new JedisCluster(nodes, poolConfig);
    }


    public static void main(String[] args) throws Exception {
        for (String arg : args) {
            if (arg.split("=").length != 2) {
                continue;
            }
            if (arg.startsWith("key=")) {
                key = arg.split("=")[1];
            } else if (arg.startsWith("host=")) {
                host = arg.split("=")[1];
            } else if (arg.startsWith("exportType=")) {
                exportType = arg.split("=")[1];
            } else if (arg.startsWith("port=")) {
                port = Integer.valueOf(arg.split("=")[1]);
            } else {
                System.out.println("argsError " + helpInfo);
                System.exit(0);
            }
        }
        System.out.println("host=" + host + " port=" + port + " key=" + key + " exportType=" + exportType);
        connectCluser();
        long beginTime = System.currentTimeMillis();
        String keyType = cluster.type(key);
        FileWriter fw = null;
        if (!"zset".equals(keyType)) {
            System.out.println("keyTypeError->" + keyType);
            return;
        }
        try {
            fw = new FileWriter(key + ".txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String zcursor = "0";
        do {
            ScanResult<Tuple> sscanResult = cluster.zscan(key, zcursor, sp);
            zcursor = sscanResult.getStringCursor();
            for (Tuple data : sscanResult.getResult()) {
                if ("json".equals(exportType)) {
                    JSONObject dataJson = new JSONObject();
                    dataJson.put("score", data.getScore());
                    dataJson.put("value", data.getElement());
                    fw.write(dataJson + "\r\n");
                } else if ("csv".equals(exportType)) {
                    fw.write(data.getScore() + "," + data.getElement() + "\r\n");
                } else {
                    System.err.println("exportTypeError->" + exportType);
                    break;
                }
            }
        } while (!"0".equals(zcursor));

        if (null != fw) {
            fw.close();
        }
        System.out.println("useTime:" + (System.currentTimeMillis() - beginTime) / 1000);
    }
}

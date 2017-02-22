package com.huit.util;

import redis.clients.jedis.*;

import java.io.*;
import java.util.*;

/**
 * java -cp redis-cluster-manager-jar-with-dependencies.jar com.huit.util.HtmlScriptCheck host=172.20.16.48 port=5001
 * java -cp redis-cluster-manager-jar-with-dependencies.jar com.huit.util.HtmlScriptCheck host=10.16.32.62 port=29440 notSecure=eval,script isReplace=true offset=
 *
 * @author huit
 */
public class HtmlScriptCheck {
    static boolean isReplace = false;
    static int offset, port;
    static String host;

    public static String helpInfo = "host=172.20.16.48 port=5001";
    static JedisCluster cluster;

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

    public static final java.lang.String TITLE = "title";
    public static final java.lang.String DESCRIPTION = "description";
    public static final java.lang.String USER_ID = "user_id";

    static String[] notSecure = "eval(,<script>,createElement(,alert(,appendChild(,<iframe,<video,<audio,<object,<embed,<img".split(",");

    static Set<String> notSecurerSet = new HashSet<String>();
    static FileWriter fw = null;

    public static void main(String[] args) throws Exception {
        for (String arg : args) {
            if (arg.split("=").length != 2) {
                continue;
            }
            if (arg.startsWith("notSecure=")) {
                notSecure = arg.split("=")[1].split(",");
            } else if (arg.startsWith("host=")) {
                host = arg.split("=")[1];
            } else if (arg.startsWith("port=")) {
                port = Integer.valueOf(arg.split("=")[1]);
            } else if (arg.startsWith("offset=")) {
                offset = Integer.valueOf(arg.split("=")[1]);
            } else if (arg.startsWith("isReplace=")) {
                isReplace = Boolean.valueOf(arg.split("=")[1]);
            } else {
                System.out.println(helpInfo);
                System.exit(0);
            }
        }
        System.out.println("host=" + host + " port=" + port + " notSecure:" + Arrays.toString(notSecure) + " rsReplace:" + isReplace + " offset:" + offset);
        connectCluser();

        try {
            fw = new FileWriter("html-not-secure.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String index = cluster.get("s_i");
        int indexInt = Integer.valueOf(index);
        long beginTime = System.currentTimeMillis();
        for (int i = offset; i < indexInt; i++) {
            String key = "s_" + i;
            if (i % 10000 == 0) {
                System.out.println("checkIndex:" + i + " total:" + indexInt + " notSecure:" + notSecurerSet.size());
            }
            List<String> datas = cluster.hmget(key, TITLE, DESCRIPTION, USER_ID);
            String title = datas.get(0);
            String desc = datas.get(1);
            String userid = datas.get(2);
            replace(key, TITLE, userid, title);
            replace(key, DESCRIPTION, userid, desc);
        }
        System.out.println("notSecureId->size:" + notSecurerSet.size());
        fw.write("notSecureId->size:" + notSecurerSet.size() + "\r\n");
        for (String key : notSecurerSet) {
            fw.write(key + "\r\n");
        }
        fw.close();
        System.out.println("useTime:" + (System.currentTimeMillis() - beginTime) / 1000);
    }

    private static void replace(String key, String filed, String userid, String data) {
        if (isHtmlSecure(data)) {
            return;
        }
        try {
            fw.write(key + "->userid:" + userid + " " + filed + ":" + data + "\r\n");
        } catch (IOException e) {
        }
        notSecurerSet.add(key);
        if (isReplace) {
            Map map = new HashMap<String, String>();
            map.put(filed, "*");
            cluster.hmset(key, map);
        }
    }

    private static boolean isHtmlSecure(String data) {
        boolean isSecure = true;
        if (null != data) {
            for (String secure : notSecure) {
                if (data.contains(secure)) {
                    isSecure = false;
                    break;
                }
            }
        }
        return isSecure;
    }
}

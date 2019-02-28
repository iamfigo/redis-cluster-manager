package me.showstone.redis;

import tech.huit.redis.util.SystemConf;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by huit on 2017/10/24.
 */
public class BitHashTest {
    public static void main(String[] args) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader("d:/migration.error"));
        String data = br.readLine();
        String[] keyVaule = data.split(",");
        System.out.println("mapSize:" + keyVaule.length);
        Set<HostAndPort> nodes = new HashSet<HostAndPort>();

        nodes.add(new HostAndPort(SystemConf.get("REDIS_HOST"), SystemConf.get("REDIS_PORT", Integer.class)));
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(500);
        poolConfig.setMaxIdle(10);
        poolConfig.setMinIdle(1);
        poolConfig.setMaxWaitMillis(30000);
        poolConfig.setTestWhileIdle(true);
        final JedisCluster cluster = new JedisCluster(nodes, 5000, 6, poolConfig);
        Map<String, String> value = new HashMap<String, String>();
        for (int i = 0; i < 525001; i++) {
            String[] kv = keyVaule[i].split("=");
            value.put(kv[0], kv[1]);
        }

        Map<String, String> smallHash = new HashMap<String, String>();
        smallHash.put("file1", "value1");
        cluster.hmset("smallHash", smallHash);
        System.out.println("setSmallHash ok");


        cluster.hmset("bigHash", value);
        System.out.println("set bigHash ok size=" + value.size());

    }
}

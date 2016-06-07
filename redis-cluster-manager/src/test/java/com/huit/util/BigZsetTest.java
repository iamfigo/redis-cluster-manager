package com.huit.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

public class BigZsetTest {
	private static final int UID_COUNT = 100000;//10W
	private static String REDIS_HOST = SystemConf.get("REDIS_HOST");
	private static int REDIS_PORT = Integer.parseInt(SystemConf.get("REDIS_PORT"));
	private static JedisCluster cluster;
	static final int DEFAULT_TIMEOUT = 2000;
	static final int MAX_REDIRECTIONS = 25;//应该大于等于主节点数
	static {
		Set<HostAndPort> nodes = new HashSet<HostAndPort>();
		nodes.add(new HostAndPort(REDIS_HOST, REDIS_PORT));
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(1000);
		poolConfig.setMaxIdle(10);
		poolConfig.setMinIdle(1);
		poolConfig.setMaxWaitMillis(30000);
		poolConfig.setTestOnBorrow(true);
		poolConfig.setTestOnReturn(true);
		poolConfig.setTestWhileIdle(true);
		cluster = new JedisCluster(nodes, DEFAULT_TIMEOUT, MAX_REDIRECTIONS, poolConfig);
	}

	public static void main(String[] args) {
		Map<String, Double> map = new HashMap<String, Double>();
		Statistics.start();
		for (long i = 1; i < UID_COUNT; i++) {
			map.put(i + "", (double) System.currentTimeMillis());
			cluster.zadd("bigSetTest", map);
			Statistics.addCount();
		}
		Statistics.stop();
	}
}

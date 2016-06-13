package com.huit.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

/**
 * java -cp redis-cluster-manager-jar-with-dependencies.jar com.huit.util.BigZsetTest count=100000 offset=0 isDel=false
 * @author huit
 *
 */
public class BigZsetTest {
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

	private static int offset = 0;//offset
	private static int count = 100000;//测试数据容量
	private static boolean isDel = true;//是否删除

	public static void main(String[] args) {
		if (args.length == 0) {
			System.out
					.println("java -cp redis-cluster-manager-jar-with-dependencies.jar com.huit.util.BigZsetTest count=100000 offset=0 isDel=false");
			System.exit(0);
		}

		for (String arg : args) {
			if (arg.startsWith("offset=")) {
				offset = Integer.valueOf(arg.split("=")[1]);
			} else if (arg.startsWith("count=")) {
				count = Integer.valueOf(arg.split("=")[1]);
			} else if (arg.startsWith("isDel=")) {
				isDel = Boolean.valueOf(arg.split("=")[1]);
			}
		}

		Map<String, Double> map = new HashMap<String, Double>();
		Statistics.start();
		for (long i = offset; i < offset + count; i++) {
			map.clear();
			map.put(i + "", (double) System.currentTimeMillis());
			cluster.zadd("bigZSetTest", map);
			//cluster.sadd("bigSetTest", i + "");
			Statistics.addCount();
		}
		if (isDel) {
			cluster.del("bigSetTest");
		}
		Statistics.stop();
	}
}

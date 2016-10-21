package com.huit.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

/**
 * java -cp redis-cluster-manager-jar-with-dependencies.jar com.huit.util.BigZsetTest count=100000 offset=0 isDel=false
 * PC机上zset验证结果：0-100W totalSpeed:20,771.45, 100-200W totalSpeed:20,293.03 200-300W totalSpeed:20,789.16
 * 41,592.15 TestOnBorrow=false TestOnReturn=false
 * 26,980.36 TestOnBorrow=true 54.16%
 * 25,599.02 TestOnReturn=true 62.48%
 * 19,196.81 TestOnBorrow=true TestOnReturn=true 116.67%
 * @author huit
 *
 */
public class BigZsetTest {
	private static String REDIS_HOST = SystemConf.get("REDIS_HOST");
	private static int REDIS_PORT = Integer.parseInt(SystemConf.get("REDIS_PORT"));
	private static JedisCluster cluster;
	static final int DEFAULT_TIMEOUT = 2000;
	static final int MAX_REDIRECTIONS = 25;//应该大于等于主节点数
	static final int THREAD_NUM = 200;//线程数 0-100W totalSpeed:20,771.45, 100-200W totalSpeed:20,293.03
	static {
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

	private static int offset = 1000000;//offset
	private static int count = 11000000;//测试数据容量，1000W
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

		List<Thread> exportTheadList = new ArrayList<Thread>();
		Statistics.start();
		for (int i = 0; i < THREAD_NUM; i++) {
			Thread thread = new Thread(new Runnable() {
				@Override
				public void run() {
					long tId = Integer.valueOf(Thread.currentThread().getName());
					Map<String, Double> map = new HashMap<String, Double>();
					for (long i = offset + tId; i < offset + count; i += THREAD_NUM) {
						map.clear();
						map.put(i + "", (double) System.currentTimeMillis());
						cluster.zadd("bigZSetTest", map);
						//cluster.sadd("bigSetTest", i + "");
						Statistics.addCount();
					}
				}
			}, "" + i);
			exportTheadList.add(thread);
			thread.start();
		}
		for (Thread thread : exportTheadList) {
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

		if (isDel) {
			cluster.del("bigZSetTest");
		}
		Statistics.stop();
	}
}

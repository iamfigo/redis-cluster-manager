package tech.huit.redis.util;


import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author huit
 */
public class ClusterStudy {

	public static void main(String[] args) throws Exception {
		Set<HostAndPort> nodes = new HashSet<HostAndPort>();
		nodes.add(new HostAndPort(SystemConf.get("REDIS_HOST"), SystemConf.get("REDIS_PORT",Integer.class)));
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(500);
		poolConfig.setMaxIdle(10);
		poolConfig.setMinIdle(1);
		poolConfig.setMaxWaitMillis(30000);
		poolConfig.setTestWhileIdle(true);
		final JedisCluster cluster = new JedisCluster(nodes, 5000, 6, poolConfig);
		cluster.wait();
		System.out.println(cluster.waitReplicas(2,5));
	}

}

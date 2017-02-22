package com.huit.util;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * stage
 * java -cp redis-cluster-manager-jar-with-dependencies.jar com.huit.util.XssAttackUids host=10.16.32.62 port=29440 filePath=/home/huit/xss-attack-show-id.txt
 *
 * prod
 * java -cp redis-cluster-manager-jar-with-dependencies.jar com.huit.util.XssAttackUids host=10.17.22.4 port=29000 filePath=/home/huit/xss-attack-show-id.txt
 *
 * @author huit
 */
public class XssAttackUids {
	static boolean isReplace = false;
	static int offset, port;
	static String host, filePath;

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

	static Set<String> uidSet = new HashSet<String>();

	public static void main(String[] args) throws Exception {
		for (String arg : args) {
			if (arg.split("=").length != 2) {
				continue;
			}
			if (arg.startsWith("filePath=")) {
				filePath = arg.split("=")[1];
			} else if (arg.startsWith("host=")) {
				host = arg.split("=")[1];
			} else if (arg.startsWith("port=")) {
				port = Integer.valueOf(arg.split("=")[1]);
			} else {
				System.out.println(helpInfo);
				System.exit(0);
			}
		}
		System.out.println("host=" + host + " port=" + port + " filePath:" + filePath);
		connectCluser();
		long beginTime = System.currentTimeMillis();
		BufferedReader br = new BufferedReader(new FileReader(filePath));
		String sid;
		while ((sid = br.readLine()) != null) {
			List<String> datas = cluster.hmget(sid.trim(), USER_ID);
			String uid = datas.get(0);
			uidSet.add(uid);
		}
		System.out.println("uids->size:" + uidSet.size());

		FileWriter fw = null;
		try {
			fw = new FileWriter("xss-attack-uids.txt");
		} catch (IOException e) {
			e.printStackTrace();
		}
		fw.write("uid->size:" + uidSet.size() + "\r\n");
		for (String uid : uidSet) {
			fw.write(uid + "\r\n");
		}
		fw.close();
		System.out.println("useTime:" + (System.currentTimeMillis() - beginTime) / 1000);
	}
}

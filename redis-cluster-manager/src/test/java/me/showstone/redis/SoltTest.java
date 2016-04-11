package me.showstone.redis;

import redis.clients.util.JedisClusterCRC16;

public class SoltTest {
	public static void main(String[] args) {
		int solt = JedisClusterCRC16.getCRC16("u_89109058") % 16384;
		System.out.println("u_89109058-solt:" + solt);
	}
}

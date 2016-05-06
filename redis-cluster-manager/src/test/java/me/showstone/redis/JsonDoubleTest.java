package me.showstone.redis;

import com.alibaba.fastjson.JSONObject;

public class JsonDoubleTest {
	public static void main(String[] args) {
		long time = System.currentTimeMillis() / 1000;
		System.out.println("time:" + time);
		JSONObject json = new JSONObject();
		json.put("t", (double) time);
		System.out.println(json);

		double a = (Double) json.get("t");
		System.out.println("time double:" + (long) a);
	}
}

package com.huit.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** 
 * @department 成都-产品运营-商务智能-java  
 * @description 系统配置文件，支持配置文件动态修改(需要实现SystemConfModifyListener接口，并手动注册监听)
 * @author huit
 * @date 2014年10月23日 下午2:54:41 
 */

public class SystemConf {
	private static Map<String, String> confMap = new HashMap<String, String>();
	public static boolean isWindos = "windows".equals(System.getProperties().get("sun.desktop"));
	public static String confFileDir;// 配置文件加载目录
	private static String filePath;
	static {
		filePath = Thread.currentThread().getContextClassLoader().getResource("conf.properties").getPath()
				.replaceAll("%20", " ");
		int index = filePath.indexOf("jar!");
		if (index > 0) {//jar文件
			filePath = filePath.substring(5, filePath.lastIndexOf('/', index)) + "/conf.properties";
		}
		if (isWindos) {
			filePath = filePath.substring(1);//Paths.get(confFileDir)如果是windos下面不去掉斜杠会导致异常
		}
		confFileDir = filePath.substring(0, filePath.lastIndexOf('/') + 1);
		loadProperties();
		//		startMonitor();
	}

	/** 
	 * @description 加载配置文件
	 * @author huit
	 * @date 2014年10月23日 上午10:58:58 
	 */
	private static void loadProperties() {
		Properties prop = new Properties();
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(filePath);// 属性文件输入流
			prop.load(fis);// 将属性文件流装载到Properties对象中
			fis.close();// 关闭流
			confMap.clear();
			Enumeration<?> enumeration = prop.propertyNames();
			while (enumeration.hasMoreElements()) {
				String name = (String) enumeration.nextElement();
				confMap.put(name, prop.getProperty(name).trim());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 根据操作系统类型得到相应的配置值
	 * 
	 * @param key
	 * @return
	 */
	public static String getBySystem(String key) {
		if (isWindos) {
			key += "Windows";
		} else {
			key += "Linux";
		}
		return confMap.get(key);
	}

	public static String get(String key) {
		return confMap.get(key);
	}

	@SuppressWarnings("unchecked")
	public static <T> T get(String key, Class<T> class_) {
		if (class_.equals(Integer.class)) {
			return (T) Integer.valueOf(confMap.get(key));
		} else if (class_.equals(Boolean.class)) {
			return (T) Boolean.valueOf(confMap.get(key));
		}
		return null;
	}

	public static void main(String[] args) throws IOException {
		long begin = System.currentTimeMillis();
		System.out.println(get("voipSipConfTransport"));
		System.out.println("query use time:" + (System.currentTimeMillis() - begin));
		System.in.read();
	}

	public static Map<String, String> getConfMap() {
		return confMap;
	}
}

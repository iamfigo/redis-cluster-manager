package com.huit.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

/**
 * java -cp redis-cluster-util-jar-with-dependencies.jar com.jumei.util.MonitorUtil D:/monitor_redis_20161018093402.log
 * 
 * @author huit
 *
 */
public class MonitorUtil {
	static int cmdTotal = 0;
	static Map<String, AtomicLong> cmdStat = new HashMap<String, AtomicLong>();
	static Map<String, AtomicLong> hostStat = new HashMap<String, AtomicLong>();
	static Map<String, AtomicLong> keyStat = new HashMap<String, AtomicLong>();
	static List<String> cmdList = new ArrayList<String>();
	static boolean isCmdDetail = false, isKeyStat;
	static int showTop = 10;
	static String filePath = "", ipFilter = "", cmdFilter = "";

	public static void main(String[] args) throws Exception {
		//args = "filePath=D:/redislog/monitor_redis_20161021093703.log".split(" ");
		//args = "filePath=D:/redislog/ ipFilter=10.1.29.41 keyStat=false isCmdDetail=false".split(" ");
		args = "filePath=D:/redislog/  cmdFilter=ZREVRANGE keyStat=false isCmdDetail=true showTop=10".split(" ");
		//args = "filePath=D:/redislog/ ipFilter=10.0.238.18 cmdFilter=ZREVRANGE cmdDetailPrint=true".split(" ");
		//args = "filePath=D:/redislog/monitor_redis_20161021093703.log ipFilter=10.0.238.18".split(" ");
		AtomicLong timeBegin, timeEnd;
		for (String arg : args) {
			if (arg.startsWith("filePath=")) {
				filePath = arg.split("=")[1];
			} else if (arg.startsWith("time=")) {
				filePath = arg.split("=")[1];
			} else if (arg.startsWith("ipFilter=")) {
				ipFilter = arg.split("=")[1];
			} else if (arg.startsWith("cmdFilter=")) {
				cmdFilter = arg.split("=")[1];
			} else if (arg.startsWith("isCmdDetail=")) {
				isCmdDetail = Boolean.valueOf(arg.split("=")[1]);
			} else if (arg.startsWith("isKeyStat=")) {
				isKeyStat = Boolean.valueOf(arg.split("=")[1]);
			} else if (arg.startsWith("showTop=")) {
				showTop = Integer.valueOf(arg.split("=")[1]);
			}
		}
		System.out.println("filePath=" + filePath + " ipFilter=" + ipFilter + " cmdFilter=" + cmdFilter + " isKeyStat="
				+ isKeyStat + " isCmdDetail=" + isCmdDetail);

		File dir = new File(filePath);
		if (dir.isDirectory()) {
			for (File file : dir.listFiles()) {
				loadData(file);
			}
		} else if (dir.isFile()) {
			loadData(dir);
		}

		if ("".equals(cmdFilter)) {
			printStat(cmdStat);
		}
		if ("".equals(ipFilter)) {
			printStat(hostStat);
		}
		printStat(keyStat);
		if (!cmdList.isEmpty()) {
			int showCount = 0;
			for (String cmdInfo : cmdList) {
				System.out.println(cmdInfo);
				showCount++;
				if (showCount > showTop) {
					break;
				}
			}
		}
	}

	public static void loadData(File file) throws IOException, FileNotFoundException {
		String data = null, key = null;
		//1476754442.972956 [0 10.0.238.18:9131] "PING"
		BufferedWriter bw = new BufferedWriter(new FileWriter(file + ".stat"));
		BufferedReader br = new BufferedReader(new FileReader(file));
		while ((data = br.readLine()) != null) {
			if ("OK".equals(data)) {
				continue;
			}
			int hostBegin = data.indexOf("[");
			int hostEnd = data.indexOf("]");

			double time;
			String clientIp = null;
			String clientIpPort;
			String cmdDetail = null;
			String[] cmdInfo = null;
			if (hostBegin > 0 && hostBegin > 0) {
				time = Double.valueOf(data.substring(0, hostBegin));
				clientIpPort = data.substring(hostBegin + 1, hostEnd).split(" ")[1];
				clientIp = clientIpPort.split(":")[0];
				cmdDetail = data.substring(hostEnd + 2);
				cmdInfo = cmdDetail.split(" ");
			}

			if (null != ipFilter && !clientIp.startsWith(ipFilter)) {
				continue;//只统计指定主机
			}

			if (cmdInfo.length >= 1) {
				key = cmdInfo[0].replace("\"", "");
				if (null != cmdFilter && !key.startsWith(cmdFilter)) {
					continue;//只统计指定命令
				}

				cmdTotal++;

				if (isKeyStat) {
					addstat(keyStat, cmdInfo[1]);
				}
				if (isCmdDetail) {
					cmdList.add(cmdDetail);
				}
				addstat(cmdStat, key);
				addstat(hostStat, clientIp);
			} else {
				System.out.println();
			}
		}
		bw.close();
		br.close();
	}

	public static void addstat(Map<String, AtomicLong> stat, String key) {
		AtomicLong count = stat.get(key);
		if (null == count) {
			count = new AtomicLong();
			stat.put(key, count);
		}
		count.incrementAndGet();
	}

	public static void printStat(Map<String, AtomicLong> cmdStat) {
		if (cmdStat.isEmpty()) {
			return;
		}
		String cmd;
		List<Entry<String, AtomicLong>> arrayList = new ArrayList<Entry<String, AtomicLong>>(cmdStat.entrySet());
		Collections.sort(arrayList, new Comparator<Object>() {
			@SuppressWarnings("unchecked")
			public int compare(Object o1, Object o2) {
				Map.Entry<String, AtomicLong> obj1 = (Map.Entry<String, AtomicLong>) o1;
				Map.Entry<String, AtomicLong> obj2 = (Map.Entry<String, AtomicLong>) o2;

				if (obj2.getValue().longValue() > obj1.getValue().longValue()) {
					return 1;
				} else if (obj1.getValue().longValue() == obj2.getValue().longValue()) {
					return 0;
				} else {
					return -1;
				}
			}
		});

		Iterator<Entry<String, AtomicLong>> it = arrayList.iterator();
		double hitRatio = 0;
		int showCount = 0;
		while (it.hasNext()) {
			Entry<String, AtomicLong> entry = it.next();
			cmd = entry.getKey();
			Long count = entry.getValue().longValue();
			if (count > 0) {
				hitRatio = count / 0.01 / cmdTotal;
			}
			System.out.println(cmd + "->" + count + "(" + new DecimalFormat("#0.00").format(hitRatio) + "%)");
			showCount++;
			if (showCount > showTop) {
				break;
			}
		}
		System.out.println();
	}
}

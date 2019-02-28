package tech.huit.redis.util;

import java.text.DecimalFormat;

public class Statistics implements Runnable {
	public static long disposedAddCount;// 处理计数
	public static long disposedLastCount;// 处理计数
	public static long externalLastCount;// 外部处理计数
	public static long exceptionCount;// 处理计数
	public static long startTime = System.currentTimeMillis();// 已处理总数
	public static long disposedTotalCount;// 已处理总数 AutoLong
	public static long lastCountTime = System.currentTimeMillis();// 上次计数时间

	private static boolean isStop = false;

	public static void start() {
		new Thread(new Statistics(), "statistics").start();
	}

	public static void stop() {
		isStop = true;
	}

	@Override
	public void run() {
		int speedCalcCount = 0;
		DecimalFormat formatDouble = new DecimalFormat("#,##0.00");//格式化设置  
		while (!isStop) {
			try {
				Thread.sleep(1000);
				speedCalcCount++;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			long useTime = System.currentTimeMillis() - lastCountTime;
			long totalUseTime = System.currentTimeMillis() - startTime;
			long disposedAddCount = disposedTotalCount - disposedLastCount;
			disposedLastCount = disposedTotalCount;
			lastCountTime = System.currentTimeMillis();
			String thisSpeed = formatDouble.format((disposedAddCount / (useTime / 1000.0)));
			String totalSpeed = formatDouble.format((disposedTotalCount / (totalUseTime / 1000.0)));

			if (speedCalcCount == 1) {//5秒打印一次
				String msg = " thisSpeed:" + thisSpeed + " totalSpeed:" + totalSpeed + " totalCount:"
						+ disposedTotalCount + " totalTime:" + totalUseTime + "ms";
				System.out.println(msg);
				speedCalcCount = 0;
			}
		}
	}

	/**
	 * 测试过只能使用同步方法，同步块的方式是不行的
	 */
	public static synchronized void addCount() {
		disposedTotalCount++;
	}

	/**
	 * 测试过只能使用同步方法，同步块的方式是不行的
	 */
	public static synchronized void addCount(long count) {
		disposedTotalCount += count;
	}

	public static synchronized void addExceptionCount() {
		exceptionCount++;
	}
}

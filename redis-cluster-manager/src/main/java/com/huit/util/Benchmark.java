package com.huit.util;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

/**
 * java -cp redis-cluster-manager-jar-with-dependencies.jar com.jumei.util.Benchmark key "驱动有bug，重新建立连接后可以连上" 0 100000 10
 *
 * @author huit
 */
public class Benchmark {
    private static JedisCluster cluster;
    private static String helpInfo = "java -jar redis-cluster-util-jar-with-dependencies.jar key value offset limit threadNum set/get/all";
    private static AtomicLong writeCount = new AtomicLong();
    private static AtomicLong lastWriteCount = new AtomicLong();
    private static AtomicLong readCount = new AtomicLong();
    private static AtomicLong lastReadCount = new AtomicLong();
    private static AtomicLong readNotFoundCount = new AtomicLong();
    private static long writeBeginTime = System.currentTimeMillis(), readBeginTime, lastCountTime;
    private static long totalCount;

    public static void main(String[] args) {
        if (args.length > 0 && args[0].equals("h")) {
            System.out.println(helpInfo);
        }
        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
        JedisPoolConfig pool = new JedisPoolConfig();
        pool.setMaxTotal(100);
        HostAndPort hap = new HostAndPort(SystemConf.get("REDIS_HOST"), SystemConf.get("REDIS_PORT", Integer.class));
        jedisClusterNodes.add(hap);
        cluster = new JedisCluster(jedisClusterNodes, pool);

        if (args.length >= 5) {
            String key = args[0];
            String value = args[1];
            long offset = Long.parseLong(args[2]);
            long limit = Long.valueOf(args[3]);
            int threadNum = Integer.valueOf(args[4]);
            String optType = "all";
            if (args.length == 6) {
                optType = args[5];
            }
            totalCount = limit - offset;
            for (int i = 0; i < threadNum; i++) {
                String threadName = "thread_" + (10 + i);
                TestClass thread = new TestClass(threadName, key, value, offset + i * limit / threadNum, offset + (i + 1)
                        * limit / threadNum, optType);
                new Thread(thread).start();
            }
        } else {
            System.out.println(helpInfo);
        }
    }

    static class TestClass implements Runnable {
        private String threadName;
        private String key;
        private String value;
        private long offset;
        private long limit;
        private String optType;

        public TestClass(String threadName, String key, String value, long offset, long limit, String optType) {
            this.key = key;
            this.value = value;
            this.offset = offset;
            this.limit = limit;
            this.threadName = threadName;
            this.optType = optType;
        }

        public void run() {
            if ("set".equals(optType) || "all".equals(optType)) {
                set();
            }
            if ("get".equals(optType) || "all".equals(optType)) {
                get();
            }
        }

        private void get() {
            readBeginTime = System.currentTimeMillis();
            for (long i = offset; i <= limit; i++) {
                String value = cluster.get(key + i);
                if (value == null) {
                    System.out.println("key:" + key + i + " not found!");
                    readNotFoundCount.incrementAndGet();
                }
                long count = readCount.incrementAndGet();
                if (count % 50000 == 0) {
                    if (lastCountTime > 0) {
                        long useTime = System.currentTimeMillis() - lastCountTime;
                        System.out.println("get count:" + count + " speed:"
                                + ((count - lastReadCount.get()) / (useTime / 1000.0)));
                    }
                    lastCountTime = System.currentTimeMillis();
                    lastReadCount.set(count);
                }
            }
            if (readCount.get() == totalCount) {
                long useTime = System.currentTimeMillis() - readBeginTime;
                System.out.println("get total:" + totalCount + " speed:" + totalCount / (useTime / 1000.0));
                System.out.println("read not found count:" + readNotFoundCount);
            }
        }

        private void set() {
            int errorCount = 0;
            long lastBreakTime = 0;
            boolean isWriteSuccess = false;
            String writeResult;
            for (long i = offset; i <= limit; i++) {
                do {
                    try {
                        isWriteSuccess = false;
                        writeResult = cluster.set(key + i, value);//节点可能关闭
                        if ("OK".equals(writeResult)) {
                            isWriteSuccess = true;
                        } else {
                            System.out.println("write:" + key + i + " writeResult :" + writeResult);
                        }
                        errorCount = 0;
                        long count = writeCount.incrementAndGet();
                        if (count % 50000 == 0) {
                            if (lastCountTime > 0) {
                                long useTime = System.currentTimeMillis() - lastCountTime;
                                System.out.println("set count:" + count + " speed:"
                                        + ((count - lastWriteCount.get()) / (useTime / 1000.0)));
                            }
                            lastCountTime = System.currentTimeMillis();
                            lastWriteCount.set(count);
                        }
                    } catch (Throwable e) {
                        //e.printStackTrace();
                        errorCount++;
                        if (lastBreakTime == 0) {
                            lastBreakTime = System.currentTimeMillis();
                        }
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                        System.out.println("write:" + key + i + " error count:" + errorCount);
                        i--;
                    }
                } while (!isWriteSuccess);
                if (lastBreakTime > 0) {
                    System.out.println(threadName + " reconnect use time:"
                            + (System.currentTimeMillis() - lastBreakTime));
                    lastBreakTime = 0;
                }
            }

            if (writeCount.get() == totalCount) {
                long useTime = System.currentTimeMillis() - writeBeginTime;
                System.out.println("set total:" + totalCount + " speed:" + totalCount / (useTime / 1000.0));
            }
        }
    }
}

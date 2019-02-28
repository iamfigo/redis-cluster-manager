package tech.huit.redis.util;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

/**
 * java -cp redis-cluster-manager-jar-with-dependencies.jar TomcatAccessLogUtil key=‪D://tomcat_acclesss_log.txt maxCountLine=1000 showTop=100
 * java -cp redis-cluster-manager-jar-with-dependencies.jar TomcatAccessLogUtil key=/home/jm/tomcat/logs/access_log.txt maxCountLine=1000 showTop=100
 * java -cp redis-cluster-manager-jar-with-dependencies.jar TomcatAccessLogUtil key=/home/jm/tomcat/logs/localhost_access_log.2017-02-14.txt maxCountLine=100000 showTop=100
 *
 * @author huit
 */
public class TomcatAccessLogUtil {
    static Map<String, AtomicLong> urlCount = new HashMap<String, AtomicLong>();
    static Map<String, AtomicLong> urlTimeCount = new HashMap<String, AtomicLong>();
    static Map<String, AtomicLong> urlWarnCount = new HashMap<String, AtomicLong>();
    static Map<String, AtomicLong> urlWarnTimeCount = new HashMap<String, AtomicLong>();
    static int showTop = 200, warnTime = 500;
    static String filePath = "";
    static long maxCountLine = Long.MAX_VALUE;
    static long readLine = 0, countLine = 0, readWarnCount = 0;
    public static String helpInfo = "filePath=D://tomcat_acclesss_log.txt maxCountLine=1000 showTop=100 warnTime=500";

    public static void main(String[] args) throws Exception {
        //args = "key=D:/redislog/monitor_redis_20161021093703.log".split(" ");
        //args = "key=D:/redislog/ ipFilter=10.1.29.41 keyStat=false isCmdDetail=false".split(" ");
        //args = "key=D:/redislog/  keyStat=false isCmdDetail=true showTop=10".split(" ");
        //args = "key=D:/redislog/ cmdFilter=ZREVRANGE isKeyStat=true isCmdDetail=true showTop=1000".split(" ");
        //args = "key=D:/redislog/  cmdFilter=ZREVRANGE keyStat=false isCmdDetail=true showTop=10".split(" ");
        //args = "key=D:/redislog/ ipFilter=10.0.238.18 isCmdDetail=true".split(" ");
        //args = "key=D://tomcat_acclesss_log.txt maxCountLine=100 showTop=100".split(" ");
        //args = helpInfo.split(" ");
        for (String arg : args) {
            if (arg.split("=").length != 2) {
                continue;
            }
            if (arg.startsWith("filePath=")) {
                filePath = arg.split("=")[1];
            } else if (arg.startsWith("showTop=")) {
                showTop = Integer.valueOf(arg.split("=")[1]);
            } else if (arg.startsWith("warnTime=")) {
                warnTime = Integer.valueOf(arg.split("=")[1]);
            } else if (arg.startsWith("maxCountLine=")) {
                maxCountLine = Long.valueOf(arg.split("=")[1]);
            } else {
                System.out.println("unknow arg:" + arg);
                System.out.println(helpInfo);
                System.exit(0);
            }
        }
        System.out.println("filePath=" + filePath + " maxCountLine=" + maxCountLine + " showTop=" + showTop + " warnTime=" + warnTime);

        long beginTime = System.currentTimeMillis();
        File dir = new File(filePath);
        if (dir.isDirectory()) {
            for (File file : dir.listFiles()) {
                loadData(file);
            }
        } else if (dir.isFile()) {
            loadData(dir);
        }
        printStat();
        printWarnStatMap();
        System.out.println("readLine:" + readLine + " countLine:" + countLine + " useTime:" + (System.currentTimeMillis() - beginTime));
    }

    public static void printStat() {
        printStatMap(urlCount);
    }

    public static void parseData(String data) {
        String[] infos = data.split(" ");
        int index = infos[6].indexOf("?");
        String url;
        if (index > 0) {
            url = infos[6].substring(0, index);
        } else {
            url = infos[6];
        }

        if (data.indexOf("HTTP/1.1 404") > 0 || data.indexOf("favicon.ico") > 0) {
            return;
        }

        if ("GET".equals(url) || "POST".equals(url) || "/".equals(url)) {
            //System.out.println("parseErrorData:" + data);
            return;
        }

        addstat(urlCount, url);
        int time = 0;
        try {
            time = Integer.valueOf(infos[10]);
            if (time > warnTime) {
                System.out.println("warnTime->data:" + data);
                addstat(urlWarnCount, url);
                addstat(urlWarnTimeCount, url, time);
                readWarnCount++;
            }
        } catch (Exception e) {
            System.out.println("time parse error:" + infos[10]);
        }
        addstat(urlTimeCount, url, time);
        countLine++;
    }


    public static void loadData(File file) throws IOException, FileNotFoundException {
        String data = null, key = null;
        BufferedReader br = new BufferedReader(new FileReader(file));
        while ((data = br.readLine()) != null) {
            readLine++;
            if (readLine > maxCountLine) {
                break;
            }
            parseData(data);
        }
        br.close();
    }

    public static void addstat(Map<String, AtomicLong> stat, String key) {
        addstat(stat, key, 1);
    }

    public static void addstat(Map<String, AtomicLong> stat, String key, int value) {
        AtomicLong count = stat.get(key);
        if (null == count) {
            count = new AtomicLong();
            count.set(value);
            stat.put(key, count);
        }
        count.addAndGet(value);
    }

    public static void printStatMap(Map<String, AtomicLong> cmdStat) {
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
        FileWriter fw = null;
        try {
            fw = new FileWriter("tomcat_access_stat.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
        while (it.hasNext()) {
            Entry<String, AtomicLong> entry = it.next();
            cmd = entry.getKey();
            Long count = entry.getValue().longValue();
            if (count > 0) {
                hitRatio = count * 100.0 / countLine;
            }
            DecimalFormat fmt = new DecimalFormat("#0.00");
            double avgTime = (urlTimeCount.get(cmd).get() / 1.0 / count);

            System.out.println(cmd + "->count:" + count + " avgTime:" + fmt.format(avgTime) + "(" + fmt.format(hitRatio) + "%");
            String csv = cmd + "," + count + "," + fmt.format(avgTime) + "," + fmt.format(hitRatio) + "\r\n";
            try {
                fw.write(csv);
            } catch (IOException e) {
                e.printStackTrace();
            }
            showCount++;
            if (showCount > showTop) {
                break;
            }
        }
        try {
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println();
    }

    public static void printWarnStatMap() {
        System.out.println();
        System.out.println("warnTimeCount-->");
        List<Entry<String, AtomicLong>> arrayList = new ArrayList<Entry<String, AtomicLong>>(urlWarnCount.entrySet());
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
        FileWriter fw = null;
        try {
            fw = new FileWriter("tomcat_access_warn.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String cmd;
        while (it.hasNext()) {
            Entry<String, AtomicLong> entry = it.next();
            cmd = entry.getKey();
            Long count = entry.getValue().longValue();
            if (count > 0) {
                hitRatio = urlCount.get(cmd).get() * 100.0 / countLine;
            }
            DecimalFormat fmt = new DecimalFormat("#0.00");
            double avgTime = (urlWarnTimeCount.get(cmd).get() / 1.0 / count);
            double avgTotal = (urlTimeCount.get(cmd).get() / 1.0 / urlCount.get(cmd).get());
            double warnRatio = (count * 100.0 / urlCount.get(cmd).get());

            System.out.println(cmd + "->count:" + count + " avgTime:" + fmt.format(avgTime) + " avgTotal:" + fmt.format(avgTotal) + " warnRatio:" + fmt.format(warnRatio) + "% urlRatio:" + fmt.format(hitRatio) + "%");
            String csv = cmd + "," + count + "," + fmt.format(avgTime) + "," + fmt.format(warnRatio) + "," + fmt.format(hitRatio) + "\r\n";
            try {
                fw.write(csv);
            } catch (IOException e) {
                e.printStackTrace();
            }
            showCount++;
            if (showCount > showTop) {
                break;
            }
        }
        try {
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println();
    }
}

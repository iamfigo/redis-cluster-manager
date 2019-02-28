package tech.huit.redis.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

/**
 * java -cp redis-cluster-util-jar-with-dependencies.jar com.jumei.util.AofUtil /root/onlinebakup/appendonly
 * 
 * @author huit
 *
 */
public class AofUtil {
	private static String filePath = "D:/appendonly";

	public static void main(String[] args) throws Exception {
		if (args.length > 0) {
			filePath = args[0];
		}
		BufferedReader br = new BufferedReader(new FileReader(filePath + ".aof"));
		String data = null, cmd, len, key = null;
		BufferedWriter bw = new BufferedWriter(new FileWriter(filePath + ".export"));
		int cmdLineTotal = 0, cmdLineReadCount = 0;

		boolean isCmd = false, isKey = false, isMatch = false, isMatchFiled = false;
		while ((data = br.readLine()) != null) {
			if (isMatch && data.equals("major_pic")) {
				len = br.readLine();
				data = br.readLine();
				if (!data.startsWith("{")) {
					data = key + "->major_pic->" + data;
					bw.write(data);
					bw.write('\r');
					bw.write('\n');
				} else {
					continue;
				}
			}

			if (isMatch && data.equals("item_pic")) {
				len = br.readLine();
				data = br.readLine();
				if (data.startsWith("[") && !data.contains("{")) {
					data = key + "->item_pic->" + data;
					bw.write(data);
					bw.write('\r');
					bw.write('\n');
				} else {
					continue;
				}
			}

			if (data.startsWith("*")) {
				try {
					cmdLineTotal = 0;
					cmdLineTotal = Integer.valueOf(data.substring(1));
				} catch (Exception e) {
					continue;
				}
				len = br.readLine();
				cmd = br.readLine();
				len = br.readLine();
				key = br.readLine();
				if (cmd.endsWith("HMSET") && key.startsWith("s_9") && key.length() == 6) {
					isMatch = true;
				} else {
					isMatch = false;
				}
			}
		}
		bw.close();
		br.close();
	}
}

package me.showstone.redis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestKey2IndexId {
	public static void main(String[] args) {
		List<String> index = new ArrayList<String>();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("d:/keys.txt"));
			String data = null;
			while ((data = br.readLine()) != null) {
				for (String id : data.split(",")) {
					index.add(id);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != br) {
					br.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println(index);
	}
}

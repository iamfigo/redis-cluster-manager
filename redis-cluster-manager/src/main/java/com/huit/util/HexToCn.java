package com.huit.util;


/**
 * redis 16进制字符串转换工具
 */
public class HexToCn {
    public static void main(String[] args) {
        String data = "abc\\xe5\\xaf\\xb9\\xe7\\xa7\\x81\\xe5\\x9f\\xba\\xe6\\x9c\\xac\\xe6\\x88\\xb7xxx";
//        String data = "\"accountName\\\":\\\"\\xe5\\xaf\\xb9\\xe7\\xa7\\x81-\\xe5\\x8a\\xa0\\xe6\\xb2\\xb9\\xe6\\x88\\xb7\\\",\\\"accountNo\\\":\\\"200100202110001000006400001\\\"";
        System.out.println(redisString(data));
    }

    /**
     * redis 16进制字符串转换
     *
     * @param s
     * @return
     */
    public static String redisString(String s) {
        char[] chars = s.toCharArray();
        StringBuilder normal = new StringBuilder();
        StringBuilder hex = new StringBuilder();
        for (int i = 0; i < chars.length; i++) {
            if (chars[i] == '\\' && chars[i + 1] == 'x') {
                hex.append(chars[i + 2]).append(chars[i + 3]);
                i += 3;
            } else {
                if (hex.length() > 0) {
                    normal.append(toStringHex(hex.toString()));
                    hex = new StringBuilder();
                }
                normal.append(chars[i]);
            }
        }
        if (hex.length() > 0) {
            normal.append(toStringHex(hex.toString()));
        }
        return normal.toString();
    }

    // 转化十六进制编码为字符串
    public static String toStringHex(String s) {
        byte[] baKeyword = new byte[s.length() / 2];
        for (int i = 0; i < baKeyword.length; i++) {
            try {
                String tmp = s.substring(i * 2, i * 2 + 2);
                baKeyword[i] = (byte) (0xff & Integer.parseInt(tmp, 16));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            s = new String(baKeyword, "utf-8");//UTF-16le:Not
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        return s;
    }
}

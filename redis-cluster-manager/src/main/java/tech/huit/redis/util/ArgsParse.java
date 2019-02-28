package tech.huit.redis.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

/**
 * 参数解析工具类
 *
 * @author huit
 * @date 12/07/2018
 */
public class ArgsParse {
    /**
     * 解析入参
     *
     * @param class_       参数要设置的类
     * @param args         要解析的参数
     * @param excludesParm 要排除的参数
     * @throws Exception
     */
    public static void parseArgs(Class class_, String[] args, String... excludesParm) throws Exception {
        if (args == null || args.length == 0) {
            System.exit(1);
        }
        for (String arg : args) {
            String[] keyValue = arg.split("=");
            if (keyValue.length != 2) {
                continue;
            }


            Field field = null;
            try {
                field = class_.getDeclaredField(keyValue[0]);
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            }
            if (field.getType() == String.class) {
                field.set(keyValue[0], keyValue[1]);
            } else if (field.getType().getName().equals("[Ljava.lang.String;")) {
                field.set(keyValue[0], keyValue[1].split(","));
            } else if (field.getType().getName().equals("int")) {
                field.set(keyValue[0], Integer.parseInt(keyValue[1]));
            } else if (field.getType().getName().equals("boolean")) {
                field.set(keyValue[0], Boolean.parseBoolean(keyValue[1]));
            } else if (field.getType().getName().equals("long")) {
                field.set(keyValue[0], Long.parseLong(keyValue[1]));
            } else if (field.getType().getName().equals("java.util.Set")) {
                Set set = new HashSet();
                set.addAll(Arrays.asList(keyValue[1].split(",")));
                field.set(keyValue[0], set);
            } else if (field.getType().getName().equals("java.util.Map")) {
                Map map = new HashMap();
                for (String s : keyValue[1].split(",")) {
                    String[] kv = s.split("->");
                    if (kv.length != 2) {
                        System.err.println("参数错误：" + s);
                        System.exit(0);
                    }
                    map.put(kv[0], kv[1]);
                }
                field.set(keyValue[0], map);
            } else {
                System.out.println("unKnowType->" + field.getName() + ":" + field.getType());
            }

        }

        //print args
        Field[] fields = class_.getDeclaredFields();
        for (Field field : fields) {
            boolean isExclude = false;
            if (null != excludesParm) {
                for (String s : excludesParm) {
                    if (s.equals(field.getName())) {
                        isExclude = true;
                        break;
                    }
                }
            }
            if (!isExclude) {
                Object value = null;
                if (field.getType().getName().equals("[Ljava.lang.String;")) {
                    String[] values = (String[]) field.get(field.getName());
                    if (null != values && values.length > 0) {
                        value = Arrays.toString(values);
                    }
                } else {
                    if (!Modifier.isPublic(field.getModifiers()) || "helpInfo".equals(field.getName())) {
                        continue;
                    } else {
                        value = field.get(field.getName());
                    }
                }
                if (value == null) {
                    value = "";
                }
                System.out.println(field.getName() + "->" + value);
            }
        }
    }
}

package com.sino.dbass.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class ConfigurationManager {

    public static Properties getProperties() {
        Properties prop = new Properties();
        String rootPath = System.getProperty("user.dir").replace("\\", "/");

        System.out.println("目标目录"+rootPath);

        FileInputStream in = null;
        try {
            in = new FileInputStream(rootPath + "/prop.properties");
            prop.load(in);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(in != null){
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return prop;
    }

    static Properties prop = getProperties();

    /**
     * 获取指定key对应的value
     * @param key
     * @return
     */

    public static String getProperty(String key)  {
        return prop.getProperty(key).replaceAll("\\s","");
    }

    /**
     * 获取整数类型的配置项
     */

    public static int getInteger(String key){
        String value = prop.getProperty(key).replaceAll("\\s", "");
        return Integer.parseInt(value);
    }

    /**
     * 获取Long类型的配置项
     */

    public static Long getLong(String key){
        String value = prop.getProperty(key).replaceAll("\\s", "");
        return Long.parseLong(value);
    }

    /**
     * 获取布尔类型的配置项
     */

    public static Boolean getBoolean(String key){
        String value = prop.getProperty(key).replaceAll("\\s", "");
        return Boolean.parseBoolean(value);
    }
}

package com.imooc.bigdata.hadoop.hdfs;

import java.io.IOException;
import java.util.Properties;

/**
 * @author EaApple
 * @2020/4/7 9:34
 * description：  读取配置文件
 * 单例模式才会使用
 */
public class ParamsUtils {
    private static Properties properties =new Properties();
    /*
    *@author EaApple
    *@param  static
    *@des  静态代码块在类第一次被载入时执行
    */
    static {
        try {
            properties.load(ParamsUtils.class.getClassLoader().getResourceAsStream("wc.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static Properties getProperties() {
        return properties;
    }
    public static void main(String[] args){
        System.out.println(getProperties().getProperty("INPUT_PATH"));
    }

}


package com.imooc.bigdata.hadoop.hdfs;

import java.util.HashMap;
import java.util.Map;

/**
 * @author EaApple
 * @2020/4/6 19:41
 * description：
 * ，一篇文章，给你摘录一段，没前没后，你读不懂，因为有语境，就是语言环境存在，一段话说了什么，要通过上下文(文章的上下文)来推断。
 *
 * 子程序之于程序，进程之于操作系统，甚至app的一屏之于app，都是一个道理。
 *
 * 程序执行了部分到达子程序，子程序要获得结果，要用到程序之前的一些结果(包括但不限于外部变量值，外部对象等等)；
 *
 * app点击一个按钮进入一个新的界面，也要保存你是在哪个屏幕跳过来的等等信息，以便你点击返回的时候能正确跳回，如果不存肯定就无法正确跳回了。
 *
 * 看这些都是上下文的典型例子，理解成环境就可以，(而且上下文虽然叫上下文，但是程序里面一般都只有上文而已，只是叫的好听叫上下文。。进程中断在操作系统中是有上有下的，不过不给题主说了，免得产生新的问题)
 */

public class ImoocContext {
    private Map<Object,Object> cacheMap = new HashMap<>();
    public Map<Object,Object> getCacheMap() {
        return cacheMap;
    }
   /*
    * @Param key  单词
    * @Param value 词频
    * 写方法*/
    public void write(Object key,Object value){
        cacheMap.put(key,value);
    }
    /*
    *  读方法
    *  @Param key 单词
    *  @Param value 词频
    */
    public Object get(Object key){
        return cacheMap.get(key);
    }

}

package com.imooc.bigdata.hadoop.hdfs;

/**
 * @author EaApple
 * @2020/4/6 21:59
 * description：
 */
public interface ImoocMapper {
    /*
    *@author EaApple
    *@param  line 读取到每一行数据
    *@param  context 上下文、缓存
    */
    public void map(String line,ImoocContext context);
}

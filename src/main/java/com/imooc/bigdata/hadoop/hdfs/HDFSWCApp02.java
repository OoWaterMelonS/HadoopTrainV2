package com.imooc.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @author EaApple
 * @2020/3/31 19:57
 * description：
 * 使用api完成word的count
 *
 * 功能拆解
 * 1 独去HDFS上的文件   ==>api
 * 2 业务处理（count）:对文件中的每一行数据都要进行业务处理（按照分隔符分割）  ==>Mapper(自己封装)
 * 3 将处理结果缓存起来 ==> Context(自己封装)
 * 4 将结果输出到HDFS  ==>api
 *
 * */
public class HDFSWCApp02 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        //1 读取HDFS上的文件   ==>api

        Properties properties = ParamsUtils.getProperties();
        Path input = new Path(properties.getProperty(Constants.INPUT_PATH));

        // 获取要操作的HDFS文件系统
        FileSystem fs = FileSystem.get(URI.create(properties.getProperty(Constants.HDFS_URI)),new Configuration(),properties.getProperty(Constants.USER));

        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input,false);
        // TODO 通过反射创建对象
        Class<?> clazz=Class.forName(properties.getProperty(Constants.MAPPER_CLASS));
        // 使用多态，父类引用指向子类对象
        //  默认初始化的会是ImoocMapper 的接口实现类  com.imooc.bigdata.hadoop.hdfs.WordCountMapper
        ImoocMapper mapper = (ImoocMapper) clazz.newInstance();


//        ImoocMapper mapper = new WordCountMapper();
        ImoocContext imoocContext = new ImoocContext();

        //迭代
        while (iterator.hasNext()){
            LocatedFileStatus file = iterator.next();
            FSDataInputStream in = fs.open(file.getPath());
            /*java 基础io流
            * FSDataInputStream => 输入字符流=> 缓冲字符流
            * FSDataInputStream  -> 肯定继承自InputStream
            */
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));
            String line = "";
            while ((line = bufferedReader.readLine())!=null){
                // 2) TODO 业务处理，词频处理


                // TODO  在业务逻辑完成后将结果写到Cache中去
                mapper.map(line,imoocContext);
            }
            // 输出的流尽量都进行关闭
            bufferedReader.close();
            in.close();
        }

        // 3  TODO 将结果缓存起来  利用Map来进行缓存

        Map<Object,Object> contextMap =imoocContext.getCacheMap();

        // 4 将结果输出到HDFS  ==>api
        Path output = new Path(properties.getProperty(Constants.OUTPUT_PATH));
        // Add a slash to parent's path so resolution is compatible with URI's
        FSDataOutputStream out = fs.create(new Path(output,new Path(properties.getProperty(Constants.OUTPUT_FILE))));

        // TODO  将第三步中的存入了Map的内容输出到out中去
        Set<Map.Entry<Object,Object>> entries =  contextMap.entrySet();
        for (Map.Entry<Object,Object> entry :entries){
            // 拿到的是一个字节entry.getKey().toString()+" \t "+entry.getValue()+"\n"
            out.write((entry.getKey().toString()+" \t "+entry.getValue()+"\n").getBytes());
        }
        out.close();
        fs.close();
        System.out.println("统计词频");
    }
}

package com.imooc.bigdata.hadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.net.URI;

/**
 * @author EaApple
 * @2020/4/2 21:49
 * description：
 * * 功能拆解
 *  * 1 独去HDFS上的文件   ==>api
 *  * 2 业务处理（count）:对文件中的每一行数据都要进行业
 *  务处理（按照分隔符分割）  ==>Mapper(自己封装)
 *  * 3 将处理结果缓存起来 ==> Context(自己封装)
 *  * 4 将结果输出到HDFS  ==>api
 */
public class MyHDFSWCApp01 {
    public static void main(String[] args) throws IOException, InterruptedException {
        //1  读取HDFS上的文件
        // 1.1 拿到相应位置上文件的Path对象
        Path  myInput = new Path("/hdfsapi/test/hello.txt");
        // 1.2 拿到对hdfs操作的文件系统,作为操作的入口
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop000:8020"),new Configuration(),"hadoop");
        // 1.3 递归拿到指定path对象下的所有文件
        fs.mkdirs(new Path("/hdfsapi/test/CreateDirTest"));
        RemoteIterator<LocatedFileStatus> iterator =  fs.listFiles(myInput,true);
        while (iterator.hasNext()){
            LocatedFileStatus fileStatus = iterator.next();

            // 2 业务逻辑处理
        }

        // 3 将处理结果缓存起来
        // 4 将结果输出到HDFS
    }

}

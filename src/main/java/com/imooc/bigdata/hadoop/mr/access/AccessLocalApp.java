package com.imooc.bigdata.hadoop.mr.access;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * @author EaApple
 * @5/22/2020 9:04 AM
 * description：
 */
public class AccessLocalApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(AccessLocalApp.class);

        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);

        // 需要使用的时候再去操作
//        job.setCombinerClass(AccessReducer.class);

        // 使用自定义分区
        job.setPartitionerClass(AccessPartitioner.class);
        // 设定reduce个数
        job.setNumReduceTasks(3);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);

        FileSystem fileSystem = FileSystem.get(configuration);
        Path outPutPath = new Path("access/output/");
        if(fileSystem.exists(outPutPath)){
            fileSystem.delete(outPutPath,true);
        }

        FileInputFormat.setInputPaths(job,new Path("access/input"));
        FileOutputFormat.setOutputPath(job,new Path("access/output"));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);

    }
}

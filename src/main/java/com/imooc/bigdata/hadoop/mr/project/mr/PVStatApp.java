package com.imooc.bigdata.hadoop.mr.project.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author EaApple
 * @5/27/2020 8:39 PM
 * description：  词频统计源文件
 */
public class PVStatApp {

    //driver
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(PVStatApp.class);

        //设定map和reduce
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //设定map输出类型  key + value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设定reduce的输出类型  key+value
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);

        //如果输出文件位置存在文件了  就先删除掉
        FileSystem fileSystem = FileSystem.get(configuration);
        Path outPutPath = new Path(args[1]);
        if(fileSystem.exists(outPutPath)){
            fileSystem.delete(outPutPath,true);
        }

        //设定输入和输出文件位置
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);
    }

    //map  拆分 分类
    static  class MyMapper extends Mapper<LongWritable, Text,Text,LongWritable>{

        private Text KEY = new Text("key");
        private LongWritable ONE = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(KEY,ONE);
        }
    }
    //shuffle  清洗 聚集

    //reduce  计算统计
    static class MyReducer extends Reducer<Text,LongWritable, NullWritable,LongWritable>{

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count=0;

            for(LongWritable value:values)
                count++;
            // 写入结果
            // 将long类型的转换为longwritable
            context.write(NullWritable.get(),new LongWritable(count));
        }
    }

}

package com.imooc.bigdata.hadoop.mr.project.mr;

import com.imooc.bigdata.hadoop.mr.project.utils.ContentUtils;
import com.imooc.bigdata.hadoop.mr.project.utils.IPParser;
import com.imooc.bigdata.hadoop.mr.project.utils.LogParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * @author EaApple
 * @5/30/2020 8:42 AM
 * description：
 */
public class PageStatApp {
    //driver
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(PageStatApp.class);

        //设定map和reduce
        job.setMapperClass(PageStatApp.MyMapper.class);
        job.setReducerClass(PageStatApp.MyReducer.class);

        //设定map输出类型  key + value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设定reduce的输出类型  key+value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //如果输出文件位置存在文件了  就先删除掉
        FileSystem fileSystem = FileSystem.get(configuration);
        Path outPutPath = new Path("output/v1/pagestat");
        if(fileSystem.exists(outPutPath)){
            fileSystem.delete(outPutPath,true);
        }

        //设定输入和输出文件位置
        FileInputFormat.setInputPaths(job,new Path("input/raw/trackinfo_20130721.data"));
        FileOutputFormat.setOutputPath(job,new Path("output/v1/pagestat"));

        job.waitForCompletion(true);

    }


    static class MyMapper extends Mapper<LongWritable, Text,Text,LongWritable> {
        // 调用ip解析工具类
        // 用于存储一个key对应的变量的权重
        private LongWritable ONE = new LongWritable(1);

        private LogParser logParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logParser= new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString();
            //拿出在logparse中分类好了的map
            Map<String,String> info = logParser.parse(log);
            String url = info.get("url");

            if(StringUtils.isNotBlank(url)){
                String PageId = ContentUtils.getPageId(url);
                context.write(new Text(PageId),ONE);
            }
        }
    }

    static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count =0;
            for (LongWritable value : values){
                count++;
            }
            context.write(key,new LongWritable(count));
        }
    }
}

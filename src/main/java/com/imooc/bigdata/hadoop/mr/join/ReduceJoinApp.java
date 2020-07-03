package com.imooc.bigdata.hadoop.mr.join;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author EaApple
 * @7/4/2020 12:24 AM
 * description：
 */
public class ReduceJoinApp {

    public static void main(String[] args) {

    }

    public static class  MyMapper extends Mapper<LongWritable, Text, IntWritable,DataInfo>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            int length = splits.length;
            if(length==3){  //dept

            }
            else if (length==8){//emp

            }
        }
    }
}

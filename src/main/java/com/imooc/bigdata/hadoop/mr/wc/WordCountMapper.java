package com.imooc.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author EaApple
 * @5/18/2020 2:36 PM
 * description：
 * KEYIN:Map任务读数据的key类型。offset,是每行数据起始位置的偏移量，Long
 * VALUEIN:Map任务读数据的value类型，其实就是一行行的字符串，String
 *
 * hello  world  welcome
 * hello  welcome
 *
 * KEYOUT:map方法自定义实现输出的key的类型
 * VALUEOUT：map方法自定义实现输出的value的类型
 *
 * 词频统计：相同单词的次数
 *hadoop 自定义类型：序列化，反序列化
 * LongWritable,Text
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 把value对应的行数据按照指定的分割符拆开

        String[] words = value.toString().split("\t");

        for(String word :words){
            context.write(new Text(word),new IntWritable(1));
        }
    }
}

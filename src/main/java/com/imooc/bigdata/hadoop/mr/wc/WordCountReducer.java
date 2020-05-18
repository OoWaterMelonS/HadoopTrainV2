package com.imooc.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author EaApple
 * @5/18/2020 3:53 PM
 * description：
 * hello ,1     world 1
 * hello ,1
 */
public class WordCountReducer extends Reducer<Text,IntWritable, Text, IntWritable> {
    /*
    * hello ,1     world 1
    * hello ,1     world 1
    * welcome 1
    * map的输出到reduce端，是按照指定的key进行分发
    * reduce1: (hello ,1)(hello ,1)(hello ,1)==>(hello ,<1,1,1>)
    * reduce2: (world,1)( world 1)( world 1)==>(world ,<1,1,1>)
    * reduce3: (welcome,1) ==> (welcome ,<1>)
    */

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;

        Iterator<IntWritable> iterator = values.iterator();

        while(iterator.hasNext()){
            IntWritable value = iterator.next();
            count += value.get();
        }
        context.write(key,new IntWritable(count));
    }

}

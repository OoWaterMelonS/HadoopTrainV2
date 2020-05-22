package com.imooc.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author EaApple
 * @5/22/2020 10:26 AM
 * description:自定义分区
 */
public class AccessPartitioner extends Partitioner<Text,Access> {

    @Override
    public int getPartition(Text phone, Access access, int numPartitions) {
        if(phone.toString().startsWith("13")){
            return 0;
        }else if(phone.toString().startsWith("15")){
            return 1;
        }else {
            return 2;
        }
    }
}

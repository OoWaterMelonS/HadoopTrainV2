package com.imooc.bigdata.hadoop.hdfs;

import org.apache.commons.lang.ArrayUtils;

/**
 * @author EaApple
 * @2020/4/6 23:35
 * description：自定义wordcount  自定义实现类
 */
public class CaseIgnoreWordCountMapper implements ImoocMapper {

    @Override
    public void map(String line, ImoocContext context) {
        String[] words = line.toLowerCase().split("\t");// 一个水平制表符
        if (!ArrayUtils.isEmpty(words)){
            for (String word : words) {
                Object value = context.get(word);
                if (value == null) { // 表示没有出现过单词
                    context.write(word, 1);
                } else {// 出现过单词
                    int v = Integer.parseInt(value.toString());
                    context.write(word,v+1);// 取出单词对应的词组再加一
                }
            }
        }

    }
}

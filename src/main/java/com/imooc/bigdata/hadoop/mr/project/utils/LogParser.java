package com.imooc.bigdata.hadoop.mr.project.utils;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * @author EaApple
 * @5/27/2020 9:39 PM
 * description：  日志解析，拿出对应的一行中的一个字段
 */
public class LogParser {

    public Map<String,String> parse(String log){
        // 一次对一行日志进行划分
        IPParser ipParser = IPParser.getInstance();
        Map<String,String> info = new HashedMap();
        if(StringUtils.isNotBlank(log)){
            // 按照指定的分割符进行划分
            String[] splits = log.split("\001");
            // 取出第13个元素
            String ip = splits[13];
            String country="-";
            String province="-";
            String city="-";

            IPParser.RegionInfo regionInfo =ipParser.analyseIp(ip);
            if(regionInfo!=null){
                country = regionInfo.getCountry();
                province = regionInfo.getProvince();
                city = regionInfo.getCity();
            }
            info.put("ip",ip);
            info.put("country",country);
            info.put("province",province);
            info.put("city",city);

            String url = splits[1];
            info.put("url",url);

            String time = splits[17];
            info.put("time",time);

        }
        return info;
    }

    public Map<String,String> parse2(String log){
        // 一次对一行日志进行划分
        IPParser ipParser = IPParser.getInstance();
        Map<String,String> info = new HashedMap();
        if(StringUtils.isNotBlank(log)){
            // 按照指定的分割符进行划分
            String[] splits = log.split("\t");
            // 取出第13个元素
            String ip = splits[0];
            String country=splits[1];
            String province=splits[2];
            String city=splits[3];
            String url = splits[4];
            String time = splits[5];
            String pageId = "-";
            if(splits.length>=7){
                pageId = splits[6];
            }

            info.put("ip",ip);
            info.put("country",country);
            info.put("province",province);
            info.put("city",city);
            info.put("url",url);
            info.put("time",time);
            info.put("pageId",pageId);

        }
        return info;
    }
}

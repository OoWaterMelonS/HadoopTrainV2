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
        IPParser ipParser = IPParser.getInstance();
        Map<String,String> info = new HashedMap();
        if(StringUtils.isNotBlank(log)){
            String[] splits = log.split("\001");

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


        }

        return info;

    }
}

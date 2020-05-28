package com.imooc.bigdata.hadoop.mr.project.utils;

import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author EaApple
 * @5/28/2020 9:49 PM
 * description： 用于统计页面流量  拿到pageid
 */
public class ContentUtils {

    public static String getPageId(String url){
        String pageId = "";
        if(StringUtils.isBlank(url)){
            return pageId;
        }

        // 模式匹配代码  重要复习
        Pattern pattern = Pattern.compile("topicId=[0-9]+");
        Matcher matcher = pattern.matcher(url);

        if(matcher.find()){
            pageId = matcher.group().split("topicId=")[1];
        }
        return pageId;
    }
}

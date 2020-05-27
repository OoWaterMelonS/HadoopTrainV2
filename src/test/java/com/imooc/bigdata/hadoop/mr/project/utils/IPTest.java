package com.imooc.bigdata.hadoop.mr.project.utils;

import org.junit.Test;

/**
 * @author EaApple
 * @5/27/2020 9:35 PM
 * description： 测试将ip解析为地址
 */
public class IPTest {

    @Test
    public void ipTest(){
        IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp("125.83.65.0");
        System.out.println(regionInfo.toString());
    }
}

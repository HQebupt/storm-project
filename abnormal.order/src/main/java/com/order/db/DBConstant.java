package com.order.db;

import com.order.util.StormConf;
import org.apache.log4j.Logger;

import java.util.Map;

public class DBConstant {

    private static Logger log = Logger.getLogger(DBConstant.class);
    private static final String filePath = "argsDB.properties";
    private static Map<String, String> conf = StormConf.readPropery(filePath);

    public static final String DBURL = conf.get("DBURL");
    public static final String DBUSER = conf.get("DBUSER");
    public static final String DBPASSWORD = conf.get("DBPASSWORD");

    //测试
    public static void main(String[] args) {
        conf = StormConf.readPropery(filePath);
        System.out.println(conf);
        System.out.println(DBURL);
        System.out.println(DBUSER);
        System.out.println(DBPASSWORD);
    }
}
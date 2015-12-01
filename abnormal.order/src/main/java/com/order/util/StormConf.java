package com.order.util;

import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StormConf {
    private static Logger log = Logger.getLogger(StormConf.class);
    private static final String filePath = "argsTopo.properties";
    private static Map<String, String> conf = readPropery(filePath);

    // # 异常订购参数
    public static final String TOPONAME = conf.get("TopoName");
    public static final String ZKCFG = conf.get("ZkCfg");
    public static final String[] TOPIC = conf.get("Topic").split(",", -1);
    public static final String TABLENAME = conf.get("TableName");

    // # 常用时间参数
    public static final int TICKSECONDS = Integer.parseInt(conf.get("tickFrequencyInSeconds"));
    public static final int SCHEDULESECONDS = Integer.parseInt(conf.get("timeScheduleSeconds"));

    // # Kafka参数
    public static final String ZKROOT = conf.get("zkRoot");
    public static final String ID = conf.get("id");

    // # 输出表名称
    public static final String channelCodesTable = conf.get("channelCodesTable");
    public static final String dataWarehouseTable = conf.get("DataWarehouseTable");
    public static final String realTimeOutputTable = conf.get("RealTimeOutputTable");

    public static Map<String, String> readPropery(String filePath) {
        Properties props = new Properties();
        Map<String, String> parameters = new HashMap<String, String>();
        InputStream in;
        in = StormConf.class.getResourceAsStream("/" + filePath);
        try {
            if (new java.io.File(filePath).exists()) {
                in = new BufferedInputStream(new FileInputStream(filePath));
            }
            props.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Enumeration<?> en = props.propertyNames();
        while (en.hasMoreElements()) {
            String key = (String) en.nextElement();
            String property = props.getProperty(key);
            parameters.put(key, property);
            log.info("args topology : " + key + "=" + property);
        }
        return parameters;
    }
}


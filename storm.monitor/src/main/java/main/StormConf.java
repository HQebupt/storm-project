package main;

import org.apache.log4j.Logger;
import util.TimeConst;

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
    private static Map<String, String> args = readPropery(filePath);

    public static final String BOOKINFO = "bookInfoDir";
    // #客户端参数（INTERFACE、SOFT）
    public static final String SOFTTOPO = args.get("softTopoName");
    public static final String SOFTPATH = args.get("softPath");
    public static final String SYSERRPATH = args.get("syserrPath");
    public static final String[] SOFTTABLE = args.get("softTable").split(",",
        -1);
    // #客户端时延和成功率
    public static final String DELAYSUCCTOPO = args.get("delaySuccTopoName");
    public static final String DELAYPATH = args.get("delayPath");
    public static final String[] DELAYSUCCTABLE = args.get("delaySuccTable")
        .split(",", -1);
    // #WAP端参数（UESPAGEVISIT）
    public static final String UESTOPO = args.get("uesTopoName");
    public static final String UESPATH = args.get("uesPath");
    public static final String[] UESTABLE = args.get("uesTable").split(",", -1);
    // #ORDER端参数
    public static final String ORDTOPO = args.get("ordTopoName");
    public static final String BKPDTOPO = args.get("bookProductTopoName");
    public static final String ORDPATH = args.get("ordPath");
    public static final String BOOKPATH = args.get("bookPath");
    public static final String PRODUCTPATH = args.get("productPath");
    public static final String MPAPERPATH = args.get("mPaperPath");
    public static final String[] ORDTABLE = args.get("ordTable").split(",", -1);
    public static final String[] ORDBKPDTABLE = args.get("ordBkPdTable").split(
        ",", -1);
    // #总用户数PV参数
    public static final String TOTALTOPO = args.get("totalTopoName");
    public static final String PAGEPATH = args.get("pagePath");
    public static final String SNSPATH = args.get("snsPath");
    public static final String CLIENTPATH = args.get("clientPath");
    public static final String[] TOTALTABLE = args.get("totalTable").split(",",
        -1);
    // #图书章节阅读指标参数(CHAPTERVISIT)
    public static final String CHAPTOPO = args.get("chapTopoName");
    public static final String CHAPPATH = args.get("chapPath");
    public static final String[] CHAPTABLE = args.get("chapTable").split(",",
        -1);
    // #CELLORDER参数
    public static final String CELLTOPO = args.get("cellTopoName");
    public static final String CELLPATH = args.get("cellPath");
    public static final String[] CELLTABLE = args.get("cellTable").split(",",
        -1);

    // #Order独立一小时周期计算UV参数
    public static final String ORDUNITOPO = args.get("ordUniTopoName");
    public static final String[] ORDUNITABLE = args.get("ordUniTable").split(
        ",", -1);

    // #总用户数独立一小时周期计算UV参数
    public static final String TOTALUNITOPO = args.get("totalUniTopoName");
    public static final String[] TOTALUNITABLE = args.get("totalUniTable")
        .split(",", -1);

    // #客户端（Soft）独立一小时周期计算UV参数
    public static final String SOFTUNITOPO = args.get("softUniTopoName");
    public static final String[] SOFTUNITABLE = args.get("softUniTable").split(
        ",", -1);
    public static final String TICKETPATH = args.get("ticketPath");
    ;

    private static Map<String, String> readPropery(String filePath) {
        Properties props = new Properties();
        Map<String, String> parameters = new HashMap<String, String>();
        InputStream in;
        in = TimeConst.class.getResourceAsStream("/" + filePath);
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

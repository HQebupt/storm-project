package util;

import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TimeConst {
    private static Logger log = Logger.getLogger(TimeConst.class);
    private static final String filePath = "initConf.properties";
    private static Map<String, String> conf = readPropery(filePath);

    public static final int PERIODTIME = Integer.parseInt(conf
        .get("PERIODTIME"));
    public static final int PERIODUNIQUE = Integer.parseInt(conf
        .get("PERIODUNIQUE"));
    public static final int PERIODTIME30 = Integer.parseInt(conf
        .get("PERIODTIME30"));
    public static final int PERIODTIME15 = Integer.parseInt(conf
        .get("PERIODTIME15"));
    public static final String Cls0Time = conf.get("Cls0Time");
    public static final String UPDATE = conf.get("UPDATE");
    public static final int SigTime = Integer.parseInt(conf.get("SigTime"));
    public static final int FileTime = Integer.parseInt(conf.get("FileTime"));
    public static final int PTime1min = Integer.parseInt(conf.get("PTime1min"));
    public static final int FileDif = Integer.parseInt(conf.get("FileDif"));
    public static final int PTIME1 = Integer.parseInt(conf.get("PTIME1"));
    public static final int TOPN = Integer.parseInt(conf.get("TOPN"));
    public static final String YYMMDDHHMMSS = conf.get("YYMMDDHHMMSS");
    public static final String YYMMDDHHMM = conf.get("YYMMDDHHMM");
    public static final String MMSS = conf.get("MMSS");
    public static final String HHMM = conf.get("HHMM");
    public static final String SS = conf.get("SS");
    public static final String MM = conf.get("MM");
    public static final String DATEFORMAT = conf.get("DATEFORMAT");
    public static final String PRODUCTIDPATH = conf.get("PRODUCTIDPATH");
    public static final String PRODUCTIDMIN = conf.get("PRODUCTIDMIN");
    public static final int[] SEC50 = getInt();
    public static final String[] ERRCODE = conf.get("ERRCODE").split(",", -1);
    public static final String PROPATH = conf.get("PROPATH");
    public static final String HALFHOUR = conf.get("HALFHOUR");
    public static final String TICKETPATH = conf.get("TICKETPATH");
    public static final String TICKETMIN = conf.get("TICKETMIN");

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
            log.info("TimeConst parameters:  " + key + "=" + property);
        }
        return parameters;
    }

    private static int[] getInt() {
        String[] str = conf.get("SEC50").split(",", -1);
        int[] sec = new int[str.length];
        for (int i = 0; i < str.length; i++) {
            sec[i] = Integer.parseInt(str[i]);
        }
        return sec;
    }
}
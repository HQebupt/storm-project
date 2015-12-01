package com.order.constant;

import com.order.util.StormConf;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * Created by LiMingji on 2015/5/21.
 */
public final class Constant {
    private static Logger log = Logger.getLogger(Constant.class);

    private static final String filePath = "rulesParams.properties";
    private static Map<String, String> conf = StormConf.readPropery(filePath);

    //1分钟，用于数据库入库
    public final static int ONE_MINUTE = 1 * 60;
    //3分钟。用于统计规则5中的订购次数
    public final static int THREE_MINUTES = 3 * 60;
    //5分钟。用于定时清空sessionInfo ipInfo terminalInfo的数据
    public final static int FIVE_MINUTES = 5 * 60;
    //65分钟，用于定时清空bookreadpv的数据
    public final static int SIXTYFIVE_MINUTES = 65 * 60;
    //60分钟，用于保存用户信息UserInfo
    public final static int ONE_HOUR = 1 * 60 * 60;
    //一天
    public final static int ONE_DAY = 24 * 60 * 60;

    //规则1变化阈值
    public final static int READPV_ZERO_TIMES = 0;
    //规则2变化阈值
    public final static int READPV_ONE_TIMES = 1;
    //规则3变化阈值
    public final static int READPV_THREASHOLD = Integer.parseInt(conf.get("READPV_THREASHOLD"));
    //规则4 扣费二级渠道 变化阈值
    public final static int CHANNEL_THRESHOLD = Integer.parseInt(conf.get("CHANNEL_THRESHOLD"));
    //规则5 日渠道ID按本扣费 变化阈值
    public final static int ORDER_FEE_THRESHOLD = Integer.parseInt(conf.get("ORDER_FEE_THRESHOLD"));
    //规则6 包月订购次数 变化阈值
    public final static int ORDER_BY_MONTH_THRESHOLD = Integer.parseInt(conf.get("ORDER_BY_MONTH_THRESHOLD"));
    //规则7 完本图书订购本数
    public final static int ORDER_TIMES_THRESHOLD = Integer.parseInt(conf.get("ORDER_TIMES_THRESHOLD"));
    //规则8 连载图书订购章数 变化阈值
    public final static int ORDER_CHAPTER_TIMES_THRESHOLD = Integer.parseInt(conf.get("ORDER_CHAPTER_TIMES_THRESHOLD"));
    //规则9 session 变化阈值
    public final static int SESSION_CHANGE_THRESHOLD = Integer.parseInt(conf.get("SESSION_CHANGE_THRESHOLD"));
    //规则10 IP变化阈值
    public final static int IP_CHANGE_THRESHOLD = Integer.parseInt(conf.get("IP_CHANGE_THRESHOLD"));
    //规则11 UA信息变化阈值
    public final static int UA_CHANGE_THRESHOLD = Integer.parseInt(conf.get("UA_CHANGE_THRESHOLD"));

    //测试
    public static void main(String[] args) {
        conf = StormConf.readPropery(filePath);
        System.out.println(conf);
        System.out.println(SESSION_CHANGE_THRESHOLD);
        System.out.println(IP_CHANGE_THRESHOLD);
        System.out.println(UA_CHANGE_THRESHOLD);
    }
}

package com.order.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.order.util.FName;
import com.order.util.StreamId;
import org.apache.log4j.Logger;

import java.util.Map;


/**
 * 页面阅读Topic
 * <p/>
 * 话单格式：（接受消息）
 * 221.226.57.202|192.168.12.201|20150319201306|868||200|8DE75A03811CB989B44AD60F77AA9230.4fxCXN8E0.2.0
 * |1|14|2|http://211.140.7.183:19804/r/798287619/index.htm;jsessionid=8DE75A03811CB989B44AD60F77AA9230.4fxCXN8E0.2.0?nid=798287166&page=1&purl=%2Fr%2Fl%2Fn.jsp%3Fnid%3D798287166%26purl%3D%252Fr%252Fp%252Fbaoyue.jsp%253FsqId%253DL2%2526amp%253BdataSrcId%253D30534679%26sqId%3DL5%26dataSrcId%3D2674444&srsc=1&vt=2&ln=2219_173197__0_
 * |http://211.140.7.183:19804/r/l/r.jsp?bid=798287619&cid=798287622&
 * |SAMSUNG_GT-I7680_TD||readbook.jsp|3|99|798287166|31|127|174231|1_31_174231_36155148_4_ML2
 * |1|||||42000091134|1|0|44338d481d504e56840767a9f023318b|571|575|0000|1|null_null|240||UC
 * |2.0|19216801220119804201503192013064690000000040|0||15968578881||||798287619|798287622|0|0|1|1||0||
 * <p/>
 * 需要获取的字段：（发射消息）
 * 0. remoteIp      |   对端IP地址    (弃用)
 * 2. recordTime    |   记录时间 格式:yyMMddhhmmss
 * 6. sessionId     |   会话ID
 * 12. userAgent     |   用户原始UA信息（弃用）
 * 15. pageType      |   页面类型
 * 27. msisdn        |   阅读号
 * 33. channelCode   |   渠道代码
 * 47. bookId        |   BookID在扩展字段 1
 * 48. chapterId     |   ChapterId在扩展字段2
 * <p/>
 * Created by HuangQiang on 2015/5/19.
 */
public class PageviewSplit extends BaseBasicBolt {

    private static final long serialVersionUID = 1L;
    static Logger log = Logger.getLogger(PageviewSplit.class);

    @Override
    public void prepare(Map conf, TopologyContext context) {
        super.prepare(conf, context);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String line = input.getString(0);
        String[] words = line.split("\\|", -1);
        if (words.length >= 57) {
            String recordTime = words[2]; // Recordtime Varchar2(20)
            String sessionId = words[6];// sessionId Varchar2(255)
            String pageType = words[15];// pageType Varchar2(8)
            String msisdn = words[27];// msisdn Varchar2(20)
            String channelCode = words[33]; // channelCode Varchar2(8)
            String bookId = words[47]; //exColumn1 扩展字段1 Varchar2(255)
            String chapterId = words[48]; //exColumn2 扩展字段2

            collector.emit(StreamId.BROWSEDATA.name(), new Values(
                recordTime, sessionId, pageType, msisdn,
                channelCode, bookId, chapterId));
        } else {
            log.info("Error data: " + line);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamId.BROWSEDATA.name(),
            new Fields(FName.RECORDTIME.name(),
                FName.SESSIONID.name(), FName.PAGETYPE.name(), FName.MSISDN.name(),
                FName.CHANNELCODE.name(), FName.BOOKID.name(), FName.CHAPTERID.name()));
    }
}

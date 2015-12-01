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
 * 订购话单Topic
 * <p/>
 * 话单格式：（接受消息）
 * 42000012018|20150505165523||1|4|10928||140021346||0000|140021344|
 * 140021346||140021346|150|150|0|0|42000012018|3|||250|25|127.0.0.1|4|1||||||13776640821
 * ||||1|||||221.226.57.202|||||13776640821||13776640821|
 * <p/>
 * 需要获取的字段：（发射消息）
 * 0. msisdn      |   发起人身份ID
 * 1. recordTime  |   记录时间
 * 2. terminal    |   终端名称
 * 3. platform    |   门户类型
 * 4. OrderType   |   订购类型 1-按本  2-按章 4-包月 5-促销包
 * 5. ProductID   |   产品ID
 * 7. BookID      |   图书ID
 * 8. ChapterID   |   章节ID
 * 9. ChannelCode |   渠道ID
 * 14. RealInfoFee |   真实信息费
 * 22. provinceID  |   手机号对应的省ID    （20150527新增）
 * 24. WapIp       |   终端或网关IP
 * 39. SessionId   |   会话ID
 * 40. PromotionId |   促销活动ID
 * <p/>
 * Created by HuangQiang on 2015/5/19.
 */
public class OrderSplit extends BaseBasicBolt {

    private static final long serialVersionUID = 1L;
    static Logger log = Logger.getLogger(OrderSplit.class);

    @Override
    public void prepare(Map conf, TopologyContext context) {
        super.prepare(conf, context);
    }


    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String line = input.getString(0);
        String[] words = line.split("\\|", -1);
        if (words.length >= 49) {
            String msisdn = words[0]; // msisdnID Varchar2(20)
            String recordTime = words[1]; // Recordtime Varchar2(14)
            String terminal = words[2];// UA Varchar2(255)
            String platform = words[3];// 门户类型numbser(2)
            String orderType = words[4]; // 订购类型  number(2)
            String productID = words[5];// 产品ID Varchar2(32)
            String bookID = words[7]; // 图书ID Number(19)
            String chapterID = words[8]; // 章节ID Varchar2(32)
            String channelCode = words[9];// 渠道ID Varchar2(8)
            String cost = words[14]; // 费用 Number(12,4)
            String provinceId = words[22]; // Varchar2(16)
            String wapIp = words[24]; // IP地址 Varchar2(40)
            String sessionId = words[39];// sessionId Varchar2(255)
            String promotionid = words[40]; // 促销互动ID (废弃) 2015-06-05

            collector.emit(StreamId.ORDERDATA.name(), new Values(msisdn,
                recordTime, terminal, platform, orderType, productID, bookID, chapterID,
                channelCode, cost, provinceId, wapIp, sessionId, promotionid));
        } else {
            log.info("订单数据错误: " + line);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declareStream(StreamId.ORDERDATA.name(),
            new Fields(FName.MSISDN.name(), FName.RECORDTIME.name(),
                FName.TERMINAL.name(), FName.PLATFORM.name(),
                FName.ORDERTYPE.name(),
                FName.PRODUCTID.name(), FName.BOOKID.name(),
                FName.CHAPTERID.name(), FName.CHANNELCODE.name(),
                FName.COST.name(), FName.PROVINCEID.name(),
                FName.WAPIP.name(), FName.SESSIONID.name(),
                FName.PROMOTIONID.name()));
    }
}

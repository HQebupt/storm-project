package com.order.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.order.db.DBHelper.DBDataWarehouseBoltHelper;
import com.order.util.FName;
import com.order.util.LogUtil;
import com.order.util.StreamId;
import com.order.util.TimeParaser;


/**
 * 仓库接口。计算异常率，定时写入数据库
 * <p/>
 * 输出表结构:
 * CREATE TABLE "AAS"."RESULT_TABLE"
 * (
 * "record_time"   varchar2(8 byte),
 * "msisdn"        varchar2(32 byte),
 * "sessionid"     varchar2(40 byte),
 * "channelcode"   varchar2(40 byte),
 * "realfee"       NUMBER,
 * "rule_1"        varchar2(2 byte),
 * "rule_2"        varchar2(2 byte),
 * "rule_3"        varchar2(2 byte),
 * "rule_4"        varchar2(2 byte),
 * "rule_5"        varchar2(2 byte),
 * "rule_6"        varchar2(2 byte),
 * "rule_7"        varchar2(2 byte),
 * "rule_8"        varchar2(2 byte),
 * "rule_9"        varchar2(2 byte),
 * "rule_10"       varchar2(2 byte),
 * "rule_11"       varchar2(2 byte),
 * "rule_12"       varchar2(2 byte)
 * )
 * <p/>
 * Created by LiMingji on 2015/5/24.
 */
public class DataWarehouseBolt extends BaseBasicBolt {

    private DBDataWarehouseBoltHelper DBHelper = new DBDataWarehouseBoltHelper();

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (input.getSourceStreamId().equals(StreamId.DATASTREAM.name())) {
            handleDataStream(input);
        } else if (input.getSourceStreamId().equals(StreamId.ABNORMALDATASTREAM.name())) {
            handleAbnormalDataStream(input);
        }
    }

    //处理正常数据流
    private void handleDataStream(Tuple input) {
        String msisdn = input.getStringByField(FName.MSISDN.name());
        String sessionId = input.getStringByField(FName.SESSIONID.name());
        Long recordTime = input.getLongByField(FName.RECORDTIME.name());
        double realInfoFee = input.getDoubleByField(FName.REALINFORFEE.name());
        String channelCode = input.getStringByField(FName.CHANNELCODE.name());

        LogUtil.printLog("DataWareHouseBolt 接收正常数据流: " + msisdn + " " + recordTime + " " + realInfoFee);

        //数据入库
        DBHelper.updateData(msisdn, sessionId, channelCode,
            TimeParaser.formatTimeInSeconds(recordTime), realInfoFee, "0");
    }

    //处理异常数据流
    private void handleAbnormalDataStream(Tuple input) {
        String msisdn = input.getStringByField(FName.MSISDN.name());
        String sessionId = input.getStringByField(FName.SESSIONID.name());
        Long recordTime = input.getLongByField(FName.RECORDTIME.name());
        double realInfoFee = input.getDoubleByField(FName.REALINFORFEE.name());
        String channelCode = input.getStringByField(FName.CHANNELCODE.name());
        String rule = input.getStringByField(FName.RULES.name());

        LogUtil.printLog("DataWareHouseBolt 接收异常数据流: " + msisdn + " " + recordTime + " " + realInfoFee);

        //数据入库
        DBHelper.updateData(msisdn, sessionId, channelCode,
            TimeParaser.formatTimeInSeconds(recordTime), realInfoFee, rule);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //Do Nothing
    }
}

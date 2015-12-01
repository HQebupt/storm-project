package com.order.databean.RulesCallback;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Values;
import com.order.util.StreamId;

import java.io.Serializable;

/**
 * 发射订单数据的函数
 * <p/>
 * Created by LiMingji on 2015/5/27.
 */
public class EmitDatas implements RulesCallback, Serializable {

    BasicOutputCollector collector = null;

    public EmitDatas(BasicOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void hanleData(String msisdnId, String sessionId, Long currentTime, double realInfoFee,
                          String channelId, String productId, String rules,
                          int provinceId, int orderType, String bookId) {
        collector.emit(StreamId.ABNORMALDATASTREAM.name(), new Values(msisdnId, sessionId, currentTime,
            realInfoFee, channelId, productId, provinceId, orderType, bookId, rules));
    }
}

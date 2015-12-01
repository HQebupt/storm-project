package com.order.databean.RulesCallback;


/**
 * 自定义延迟检测。并对异常数据做处理。
 * <p/>
 * Created by LiMingji on 2015/5/23.
 */

public interface RulesCallback {

    /**
     * @param msisdnId    用户msisdnId
     * @param sessionId   订单或浏览记录所在的SeesionId
     * @param currentTime 默认为订单产生时间，如果此时间为空，则用消息到达系统的时间来代替。
     * @param realInfoFee 真实信息费
     * @param channelId   渠道Id
     * @param product     产品ID
     * @param rules       对应处理的规则。
     * @param provinceId  手机号码对应的省ID
     * @param orderType   订单类型
     * @param bookId      图书ID
     */
    public void hanleData(String msisdnId, String sessionId, Long currentTime,
                          double realInfoFee, String channelId, String product, String rules,
                          int provinceId, int orderType, String bookId);
}

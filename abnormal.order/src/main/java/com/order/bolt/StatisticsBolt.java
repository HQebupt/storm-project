package com.order.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.order.constant.Constant;
import com.order.constant.Rules;
import com.order.databean.RulesCallback.EmitDatas;
import com.order.databean.SessionInfo;
import com.order.databean.TimeCacheStructures.Pair;
import com.order.databean.TimeCacheStructures.RealTimeCacheList;
import com.order.databean.UserInfo;
import com.order.db.DBHelper.DBStatisticBoltHelper;
import com.order.util.FName;
import com.order.util.LogUtil;
import com.order.util.StreamId;
import com.order.util.TimeParaser;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.Iterator;

/**
 * Created by LiMingji on 2015/5/24.
 */
public class StatisticsBolt extends BaseBasicBolt {
    public static boolean isDebug = true;
    private static Logger log = Logger.getLogger(StatisticsBolt.class);

    private DBStatisticBoltHelper DBhelper = new DBStatisticBoltHelper();

    //存储字段为msisdn 和 UserInfo
    private RealTimeCacheList<Pair<String, UserInfo>> userInfos =
        new RealTimeCacheList<Pair<String, UserInfo>>(Constant.ONE_HOUR);

    //存储字段为seesionId 和 SessionInfo
    private RealTimeCacheList<Pair<String, SessionInfo>> sessionInfos =
        new RealTimeCacheList<Pair<String, SessionInfo>>(Constant.ONE_DAY);

    //负责SeesionInfo数据的清理
    private transient Thread cleaner = null;
    //负责每天导入维表的数据
    private transient Thread loader = null;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (cleaner == null) {
            cleaner = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            //每隔一个小时清理一次。
                            cleaner.sleep(60 * 60 * 1000L);
                            Iterator<Pair<String, SessionInfo>> iterator = sessionInfos.keySet().iterator();
                            while (iterator.hasNext()) {
                                Pair<String, SessionInfo> currentPair = iterator.next();
                                if (currentPair.getValue().isOutOfTime()) {
                                    log.info("sessionID : " + currentPair.getValue() + " is out of time");
                                    currentPair.getValue().clean();
                                    sessionInfos.remove(currentPair);
                                }
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            cleaner.setDaemon(true);
            cleaner.start();
        }
        if (loader == null) {
            //启动线程每天3点准时load数据
            loader = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            DBhelper.getData();
                            long sleepTime = TimeParaser.getMillisFromNowToThreeOclock();
                            if (sleepTime > 0) {
                                loader.sleep(sleepTime);
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            loader.setDaemon(true);
            loader.start();
        }
        if (input.getSourceStreamId().equals(StreamId.BROWSEDATA.name())) {
            LogUtil.printLog("阅读浏览话单到达StatisticBolt");
            //阅读浏览话单
            try {
                this.constructInfoFromBrowseData(input);
            } catch (Exception e) {
                log.error("阅读浏览话单数据结构异常");
                e.printStackTrace();
            }
        } else if (input.getSourceStreamId().equals(StreamId.ORDERDATA.name())) {
            // 订购话单
            LogUtil.printLog("订购话单到达StatisticBolt");
            try {
                this.constructInfoFromOrderData(input, collector);
            } catch (Exception e) {
                log.error("订购话单数据结构异常");
                e.printStackTrace();
            }
        }
    }

    private void constructInfoFromBrowseData(Tuple input) throws NumberFormatException {
        LogUtil.printLog("===========开始解析浏览话单数据===========");
        Long recordTime = TimeParaser.splitTime(input.getStringByField(FName.RECORDTIME.name()));
        String sessionId = input.getStringByField(FName.SESSIONID.name());
        String pageType = input.getStringByField(FName.PAGETYPE.name());
        String msisdn = input.getStringByField(FName.MSISDN.name());
        String channelCode = input.getStringByField(FName.CHANNELCODE.name());
        String bookId = input.getStringByField(FName.BOOKID.name());
        String chapterId = input.getStringByField(FName.CHAPTERID.name());

        LogUtil.printLog("msisdn: " + msisdn + " recordTime " + recordTime + " bookId " + bookId + " chapterId " + chapterId + " channelCode " + channelCode +
            " sessionId " + sessionId);

        if (sessionId == null || sessionId.trim().equals("")) {
            //浏览话单若无sessionId则直接丢弃。
            return;
        }

        LogUtil.printLog("接收到浏览话单数据，更新数据结构 " + msisdn + " recordTime " + new Date(recordTime));

        //更新阅读浏览话单的SessionInfos信息
        Pair<String, SessionInfo> sessionPair = new Pair<String, SessionInfo>(sessionId, null);
        if (sessionInfos.contains(sessionPair)) {
            SessionInfo currentSessionInfo = (SessionInfo) sessionInfos.get(sessionPair).getValue();
            currentSessionInfo.updateSeesionInfo(bookId, null, null, recordTime, -1, 0.0, channelCode, null, 0);
        } else {
            SessionInfo currentSessionInfo = new SessionInfo(sessionId, msisdn, bookId, null, null, recordTime, -1, 0, channelCode, null, 0);
            sessionInfos.put(new Pair<String, SessionInfo>(sessionId, currentSessionInfo));
        }
        //浏览话单不需要更新用户信息
    }

    private void constructInfoFromOrderData(Tuple input, final BasicOutputCollector collector) throws NumberFormatException {
        LogUtil.printLog("===========开始解析订单数据===========");
        String msisdn = input.getStringByField(FName.MSISDN.name());
        Long recordTime = TimeParaser.splitTime(input.getStringByField(FName.RECORDTIME.name()));
        String userAgent = input.getStringByField(FName.TERMINAL.name());
        String platform = input.getStringByField(FName.PLATFORM.name());
        String orderTypeStr = input.getStringByField(FName.ORDERTYPE.name());
        String productId = input.getStringByField(FName.PRODUCTID.name());
        String bookId = input.getStringByField(FName.BOOKID.name());
        String chapterId = input.getStringByField(FName.CHAPTERID.name());
        String channelCode = input.getStringByField(FName.CHANNELCODE.name());
        String realInfoFeeStr = input.getStringByField(FName.COST.name());
        String provinceIdStr = input.getStringByField(FName.PROVINCEID.name());
        String wapIp = input.getStringByField(FName.WAPIP.name());
        String sessionId = input.getStringByField(FName.SESSIONID.name());

        LogUtil.printLog("msisdn: " + msisdn + " recordTime " + recordTime + " UA " + userAgent
            + " platform " + platform + " orderType " + orderTypeStr + " productId " + productId +
            " bookId " + bookId + " chapterId " + chapterId + " channelCode " + channelCode + " cost " + realInfoFeeStr
            + " provinceId " + provinceIdStr + " wapId " + wapIp + " sessionId " + sessionId);

        int orderType = Integer.parseInt(orderTypeStr);
        double realInfoFee = Double.parseDouble(realInfoFeeStr);
        int provinceId = Integer.parseInt(provinceIdStr);

        if (sessionId == null || sessionId.trim().equals("")) {
            sessionId = "null" + msisdn;
        }

        LogUtil.printLog("接受到订单数据 " + msisdn + " recordTime " + new Date(recordTime));

        //所有订单数据先统一发送正常数据流。用作数据统计。
        collector.emit(StreamId.DATASTREAM.name(), new Values(msisdn, sessionId, recordTime,
            realInfoFee, channelCode, productId, provinceId, orderType, bookId));
        LogUtil.printLog("正常数据流成功发射");

        //更新订购话单的SessionInfos信息
        Pair<String, SessionInfo> sessionInfoPair = new Pair<String, SessionInfo>(sessionId, null);
        SessionInfo currentSessionInfo;
        if (sessionInfos.contains(sessionInfoPair)) {
            currentSessionInfo = (SessionInfo) sessionInfos.get(sessionInfoPair).getValue();
            currentSessionInfo.updateSeesionInfo(null, bookId, chapterId, recordTime, orderType,
                realInfoFee, channelCode, productId, provinceId);
            sessionInfos.put(new Pair<String, SessionInfo>(sessionId, currentSessionInfo));
        } else {
            currentSessionInfo = new SessionInfo(sessionId, msisdn, null, bookId,
                chapterId, recordTime, orderType, realInfoFee, channelCode, productId, provinceId);
            sessionInfos.put(new Pair<String, SessionInfo>(sessionId, currentSessionInfo));
        }

        LogUtil.printLog("开始根据各个规则对订单数据进行检测 " + msisdn + " recordTime " + new Date(recordTime));

        //检测相应的各个规则。
        currentSessionInfo.checkRule123(bookId, new EmitDatas(collector));
        currentSessionInfo.checkRule4(new EmitDatas(collector));
        currentSessionInfo.checkRule5(bookId, new EmitDatas(collector));
        currentSessionInfo.checkRule6(new EmitDatas(collector));
        currentSessionInfo.checkRule7(new EmitDatas(collector));
        currentSessionInfo.checkRule8(bookId, new EmitDatas(collector));
        currentSessionInfo.checkRule12(platform, new EmitDatas(collector));

        //更新订购话单UserInfos信息
        Pair<String, UserInfo> userInfoPair = new Pair<String, UserInfo>(msisdn, null);
        UserInfo currentUserInfo;
        if (userInfos.contains(userInfoPair)) {
            currentUserInfo = (UserInfo) userInfos.get(userInfoPair).getValue();
            currentUserInfo.upDateUserInfo(recordTime, sessionId, wapIp, userAgent);
            userInfos.put(new Pair<String, UserInfo>(msisdn, currentUserInfo));
        } else {
            currentUserInfo = new UserInfo(msisdn, recordTime, sessionId, wapIp, userAgent);
            userInfos.put(new Pair<String, UserInfo>(msisdn, currentUserInfo));
        }
        boolean[] isObeyRules = currentUserInfo.isObeyRules();
        if (!isObeyRules[UserInfo.SESSION_CHECK_BIT]) {
            collector.emit(StreamId.ABNORMALDATASTREAM.name(),
                new Values(msisdn, sessionId, recordTime, realInfoFee, channelCode, productId,
                    provinceId, orderType, bookId, Rules.NINE.name()));
        }
        if (!isObeyRules[UserInfo.IP_CHECK_BIT]) {
            collector.emit(StreamId.ABNORMALDATASTREAM.name(),
                new Values(msisdn, sessionId, recordTime, realInfoFee, channelCode, productId,
                    provinceId, orderType, bookId, Rules.TEN.name()));
        }
        if (!isObeyRules[UserInfo.UA_CHECK_BIT]) {
            collector.emit(StreamId.ABNORMALDATASTREAM.name(),
                new Values(msisdn, sessionId, recordTime, realInfoFee, channelCode, productId,
                    provinceId, orderType, bookId, Rules.ELEVEN.name()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamId.DATASTREAM.name(),
            new Fields(FName.MSISDN.name(), FName.SESSIONID.name(), FName.RECORDTIME.name(),
                FName.REALINFORFEE.name(), FName.CHANNELCODE.name(), FName.PRODUCTID.name(),
                FName.PROVINCEID.name(), FName.ORDERTYPE.name(), FName.BOOKID.name()));

        declarer.declareStream(StreamId.ABNORMALDATASTREAM.name(),
            new Fields(FName.MSISDN.name(), FName.SESSIONID.name(), FName.RECORDTIME.name(),
                FName.REALINFORFEE.name(), FName.CHANNELCODE.name(), FName.PRODUCTID.name(),
                FName.PROVINCEID.name(), FName.ORDERTYPE.name(), FName.BOOKID.name(),
                FName.RULES.name()));

    }
}

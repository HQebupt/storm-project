package com.order.databean;

import com.order.constant.Constant;
import com.order.constant.Rules;
import com.order.databean.RulesCallback.RulesCallback;
import com.order.databean.TimeCacheStructures.BookOrderList;
import com.order.databean.TimeCacheStructures.Pair;
import com.order.databean.TimeCacheStructures.RealTimeCacheList;
import com.order.db.DBHelper.DBStatisticBoltHelper;
import com.order.util.LogUtil;
import com.order.util.TimeParaser;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.Date;

/**
 * 根据浏览pv和订购pv生成SessionInfo。
 * <p/>
 * Created by LiMingji on 2015/5/21.
 */
public class SessionInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    private static Logger log = Logger.getLogger(SessionInfo.class);

    //SessionId
    private String sessionId = null;
    //包含此Session的用户id
    private String msisdnId = null;
    //真实信息费
    private double realInfoFee = 0.0;
    //渠道id 营销ID
    private String channelId = null;
    //产品ID
    private String productId = null;
    private long lastUpdateTime;

    //图书ID
    private String bookId = null;
    //手机号码对应的省ID
    private int provinceId = 0;
    //订单类型
    private int orderType = 0;

    //图书阅读浏览pv，
    private RealTimeCacheList<String> bookReadPv = new RealTimeCacheList<String>(Constant.SIXTYFIVE_MINUTES);
    //图书购买pv,
    private BookOrderList bookOrderPv = new BookOrderList();
    //图书章节购买pv，存放章节对应的bookId。
    // !!!(废弃）。现在只需要知道订购的章节属于哪本书就可以了。不需要知道章节ID
    private RealTimeCacheList<String> bookChapterOrderPv = new RealTimeCacheList<String>(Constant.FIVE_MINUTES);
    //各个渠道下的日购买费用 Pair值为用户msisdn 和 orderType=1的 信息费。
    private RealTimeCacheList<Pair<String, Double>> channelOrderpv = new RealTimeCacheList<Pair<String, Double>>(Constant.ONE_DAY);
    //用户营销Id对应的扣费二级渠道。
    private RealTimeCacheList<String> orderChannelCodeByDay = new RealTimeCacheList<String>(Constant.ONE_DAY);

    @Override

    public String toString() {
        String context = "msisdnId: " + msisdnId + " realInfoFee : " + realInfoFee + " channelId " + channelId +
            " lastUpdateTime : " + new Date(lastUpdateTime) + " orderType: " + orderType + " \n ";
        context += " 图书浏览pv：" + bookReadPv.toString() + " \n";
        context += " 图书购买pv: " + bookOrderPv.toString() + " \n";
        return context;
    }

    //对应浏览pv 和 订购pv 构建SeesionInfo
    public SessionInfo(String sessionId, String msisdnId, String bookReadId,
                       String bookOrderId, String bookChapterOrderId, Long currentTime,
                       int orderType, double realInfoFee, String channelId, String productId, int provinceId) {
        if (currentTime != null) {
            this.lastUpdateTime = currentTime;
        } else {
            this.lastUpdateTime = System.currentTimeMillis();
        }
        if (sessionId == null || msisdnId == null) {
            log.error("sessionId || msisdn should not be null");
            throw new IllegalArgumentException("sessionId || msisdn should not be null");
        }
        this.sessionId = sessionId;
        this.msisdnId = msisdnId;

        this.orderType = orderType;
        this.realInfoFee = realInfoFee;
        this.channelId = channelId;
        this.bookId = bookOrderId;

        if (productId != null) {
            this.productId = productId;
        }
        if (bookReadId != null) {
            bookReadPv.put(bookReadId, lastUpdateTime);
        }
        if (bookOrderId != null) {
            //为了方便规则7的判断。在此将orderType=21定义为批量订购
            if (orderType == 2 && bookChapterOrderId == null) {
                this.orderType = 21;
            }
            bookOrderPv.put(bookOrderId, orderType, lastUpdateTime);
        }
        if (bookChapterOrderId != null) {
            bookChapterOrderPv.put(bookOrderId, lastUpdateTime);
        }

        this.provinceId = provinceId;

        //统计orderType == 1情况下的用户日渠道信息费。
        if (orderType == 1) {
            Pair<String, Double> pair = new Pair<String, Double>(msisdnId, realInfoFee);
            if (channelOrderpv.contains(pair)) {
                Pair<String, Double> currentPair = channelOrderpv.get(pair);
                currentPair.setValue(currentPair.getValue() + realInfoFee);
                channelOrderpv.put(currentPair);
            } else {
                channelOrderpv.put(pair);
            }
        }
        //将用户channelCode对应的二级渠道进行保存
        if (DBStatisticBoltHelper.getParameterId2SecChannelId() == null) {
            DBStatisticBoltHelper.getData();
        }
        String secondChannelId = DBStatisticBoltHelper.getParameterId2SecChannelId().get(channelId);
        if (secondChannelId != null) {
            this.orderChannelCodeByDay.put(secondChannelId);
        }
        LogUtil.printLog("新数据插入: " + this);

    }

    //对已存在的SessionInfo进行更新。
    public void updateSeesionInfo(String bookReadId, String bookOrderId, String bookChapterOrderId,
                                  Long currentTime, int orderType, Double realInfoFee,
                                  String channelId, String productId, int provinceId) {
        if (currentTime != null) {
            lastUpdateTime = currentTime;
        } else {
            lastUpdateTime = System.currentTimeMillis();
        }
        if (bookReadId != null) {
            bookReadPv.put(bookReadId, lastUpdateTime);
        }
        if (bookOrderId != null) {
            if (orderType == 2 && bookChapterOrderId == null) {
                this.orderType = 21;
            }
            bookOrderPv.put(bookOrderId, orderType, lastUpdateTime);
        }
        if (bookChapterOrderId != null) {
            bookChapterOrderPv.put(bookOrderId, lastUpdateTime);
        }

        this.bookId = bookOrderId;
        this.provinceId = provinceId;
        this.orderType = orderType;
        this.realInfoFee = realInfoFee;
        this.channelId = channelId;

        if (productId != null) {
            this.productId = productId;
        }
        //统计orderType == 1情况下的用户日渠道信息费。
        if (orderType == 1) {
            Pair<String, Double> pair = new Pair<String, Double>(msisdnId, realInfoFee);
            if (channelOrderpv.contains(pair)) {
                Pair<String, Double> currentPair = channelOrderpv.get(pair);
                currentPair.setValue(currentPair.getValue() + realInfoFee);
                channelOrderpv.put(currentPair);
            } else {
                channelOrderpv.put(pair);
            }
        }
        //将用户channelCode对应的二级渠道进行保存
        if (DBStatisticBoltHelper.getParameterId2SecChannelId() == null) {
            DBStatisticBoltHelper.getData();
        }
        String secondChannelId = DBStatisticBoltHelper.getParameterId2SecChannelId().get(channelId);
        if (secondChannelId != null) {
            this.orderChannelCodeByDay.put(secondChannelId);
        }
        LogUtil.printLog("旧数据更新: " + this);
    }

    /**
     * 检测规则 1、2、3
     * 规则1：用户5分钟内，对图书有订购行为，且前1小时后5分钟内对该产品的点击pv=0 ordertype not in ( 4 5 9 99)
     * 规则2：用户5分钟内，对图书有订购行为，且前1小时后5分钟内对该产品的点击pv=1 ordertype not in ( 4 5 9 99)
     * 规则3：用户5分钟内，对图书有订购行为，且前1小时后5分钟内对该产品的点击 1<pv<=5 ordertype not in ( 4 5 9 99)
     *
     * @param bookId
     * @param callback
     */
    private transient Thread rule123Checker = null;

    public void checkRule123(final String bookId, final RulesCallback callback) {
        rule123Checker = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    LogUtil.printLog("检测规则123 " + bookId);
                    //延迟5分钟之后对65分钟内的数据进行检测。
                    long sleepTime = TimeParaser.getMillisFromTimeToNetFiveMinutes(lastUpdateTime);
                    if (sleepTime > 0) {
                        rule123Checker.sleep(sleepTime);
                    }
                    if (orderType == 4 || orderType == 5 || orderType == 9 || orderType == 99) {
                        return;
                    }
                    //根据特定图书浏览次数来判断违反的是哪条规则
                    Rules rule = null;
                    if (bookReadPv.sizeById(bookId) == Constant.READPV_ZERO_TIMES) {
                        rule = Rules.ONE;
                    } else if (bookReadPv.sizeById(bookId) == Constant.READPV_ONE_TIMES) {
                        rule = Rules.TWO;
                    } else if (bookReadPv.sizeById(bookId) <= Constant.READPV_THREASHOLD
                        && bookReadPv.sizeById(bookId) > Constant.READPV_ONE_TIMES) {
                        rule = Rules.THREE;
                    }
                    if (rule != null) {
                        LogUtil.printLog(this, rule.name(), false);
                        callback.hanleData(msisdnId, sessionId, lastUpdateTime, realInfoFee,
                            channelId, productId, rule.name(), provinceId, orderType, bookId);
                    } else {
                        LogUtil.printLog(this, rule.name(), true);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        rule123Checker.setDaemon(true);
        rule123Checker.start();
    }

    /**
     * 检测规则 4
     * 规则5：一个用户日扣费二级渠道>=3个，
     *
     * @param callback
     */
    public void checkRule4(final RulesCallback callback) {
        LogUtil.printLog("检测规则4 " + bookId);
        if (orderChannelCodeByDay.size(lastUpdateTime) >= 3) {
            callback.hanleData(msisdnId, sessionId, lastUpdateTime, realInfoFee, channelId,
                productId, Rules.FOUR.name(), provinceId, orderType, bookId);
        }
    }

    /**
     * 检测规则 5
     * 规则5：一个用户日渠道ID中的按本订购费>10元，该用户异常渠道当天所有信息费为异常
     * orderType=1
     *
     * @param callback
     */
    public void checkRule5(String msisdnId, final RulesCallback callback) {
        LogUtil.printLog("检测规则5 " + bookId);
        if (orderType != 1) {
            return;
        }
        Pair<String, Double> userChannelInfoFee = new Pair<String, Double>(msisdnId, null);
        if (channelOrderpv.contains(userChannelInfoFee)) {
            Pair<String, Integer> currentUserChannelInFee = channelOrderpv.get(userChannelInfoFee);
            if (currentUserChannelInFee.getValue() > 10) {
                LogUtil.printLog(this, "rule5 ", false);
                callback.hanleData(msisdnId, sessionId, lastUpdateTime, realInfoFee,
                    channelId, productId, Rules.FIVE.name(), provinceId, orderType, bookId);
            }
        }
    }

    /**
     * 检测规则 6
     * 规则6：用户3分钟内，包月订购>=2次
     * orderType = 4
     *
     * @param callback
     */
    public void checkRule6(final RulesCallback callback) {
        LogUtil.printLog("检测规则6 " + bookId);
        if (orderType != 4) {
            return;
        }
        int orderTimes = 0;
        for (String bookId : bookOrderPv.keySet()) {
            orderTimes += bookOrderPv.sizeOfBookOrderTimesWithOrderType(bookId, 4);
        }
        if (orderTimes >= Constant.ORDER_BY_MONTH_THRESHOLD) {
            LogUtil.printLog(this, "rule6", false);
            callback.hanleData(msisdnId, sessionId, lastUpdateTime,
                realInfoFee, channelId, productId, Rules.SIX.name(), provinceId, orderType, bookId);
        }
    }

    /**
     * 检测规则 7
     * 新版7：用户5分钟内，完本图书订购本数+批量订购本数>=2, 且对订购图书的pv<=5*本数
     * orderType = 1 || ( orderType = 2 且 chapterId == null ) 将此情况的orderType 定为 21
     *
     * @param callback
     */
    public void checkRule7(final RulesCallback callback) {
        LogUtil.printLog("检测规则7 " + bookId);
        if (orderType != 1 || orderType != 21) {
            return;
        }
        int bookOrderNums = 0;
        int bookReadPvs = 0;
        for (String bookId : bookOrderPv.keySet()) {
            bookOrderNums += bookOrderPv.sizeOfBookOrderTimesWithOrderType(bookId, 1)
                + bookOrderPv.sizeOfBookOrderTimesWithOrderType(bookId, 21);
            bookReadPvs += bookReadPv.sizeWithTimeThreshold(bookId, lastUpdateTime, Constant.FIVE_MINUTES);
        }
        if (bookOrderNums >= 2 && bookOrderNums < 5 * bookReadPvs) {
            LogUtil.printLog(this, " rule7 ", false);
            callback.hanleData(msisdnId, sessionId, lastUpdateTime, realInfoFee,
                channelId, productId, Rules.SEVEN.name(), provinceId, orderType, bookId);
        }
    }

    /**
     * 检测规则 8
     * 规则8：用户5分钟内，连载图书订购章数>=10，且对订购图书的pv<=2*章数
     * 不满10个。后续判断不触发。
     * orderType=2
     *
     * @param callback
     */
    public void checkRule8(String bookId, final RulesCallback callback) {
        LogUtil.printLog("检测规则8 " + bookId);
        if (orderType != 2) {
            return;
        }
        int orderPvs = bookOrderPv.sizeOfBookOrderTimesWithOrderType(bookId, 2);
        int readPvs = bookReadPv.sizeWithTimeThreshold(bookId, lastUpdateTime, Constant.FIVE_MINUTES);
        if (orderPvs >= 10 && orderPvs <= 2 * readPvs) {
            LogUtil.printLog(this, " rule8", false);
            callback.hanleData(msisdnId, sessionId, lastUpdateTime, realInfoFee,
                channelId, productId, Rules.EIGHT.name(), provinceId, orderType, bookId);
        }
    }

    /**
     * 规则12：用户有sessionid且属于非BOSS包月，前1小时后5分钟内图书的总pv=0
     * ordertype = 4 accesstype <> 6
     *
     * @param platform  platform = accesstype
     * @param callback
     */
    private transient Thread rule12Checker = null;

    public void checkRule12(String platform, final RulesCallback callback) {
        LogUtil.printLog("检测规则4 " + bookId);
        if (orderType != 4 || Integer.parseInt(platform) == 6) {
            return;
        }
        rule123Checker = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    rule12Checker.sleep(Constant.FIVE_MINUTES);
                    if (bookReadPv.size(lastUpdateTime) == 0) {
                        LogUtil.printLog(this, "rule12", false);
                        callback.hanleData(msisdnId, sessionId, lastUpdateTime, realInfoFee,
                            channelId, productId, Rules.TWELVE.name(), provinceId, orderType, bookId);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        rule12Checker.setDaemon(true);
        rule12Checker.start();
    }

    public void clean() {
        rule123Checker.interrupt();
        rule123Checker.interrupt();
    }

    public boolean isOutOfTime() {
        return bookOrderPv.sizeOfOrderBooks() == 0 &&
            bookReadPv.size(lastUpdateTime) == 0 &&
            channelOrderpv.size(lastUpdateTime) == 0;
    }
}
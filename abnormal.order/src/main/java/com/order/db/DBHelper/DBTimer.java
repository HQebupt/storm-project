package com.order.db.DBHelper;

import com.order.bolt.StatisticsBolt;
import com.order.constant.Constant;
import com.order.db.JDBCUtil;
import com.order.util.StormConf;
import com.order.util.TimeParaser;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by LiMingji on 2015/6/9.
 */
public class DBTimer extends Thread {
    private static Logger log = Logger.getLogger(DBTimer.class);

    private Connection conn = null;
    //Key: date|provinceId|channelCode|context|contextType|
    private ConcurrentHashMap<String, Double> totalFee = null;
    //Key: date|provinceId|channelCode|context|contextType|ruleID
    private ConcurrentHashMap<String, Double> abnormalFee = null;

    private HashMap<String, String> parameterId2ChannelIds = null;

    public DBTimer(Connection conn, ConcurrentHashMap totalFee, ConcurrentHashMap abnormalFee) {
        this.conn = conn;
        this.abnormalFee = abnormalFee;
        this.totalFee = totalFee;
        parameterId2ChannelIds = DBStatisticBoltHelper.getParameterId2ChannelIds();
    }


    @Override
    public void run() {
        super.run();
        try {
            while (true) {
                this.sleep(Constant.ONE_MINUTE * 1000L);
                //将map中的数据更新到数据库中。
                this.updateDB();
                if (TimeParaser.isTimeToClearData(System.currentTimeMillis())) {
                    totalFee.clear();
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void updateDB() {
        for (String key : abnormalFee.keySet()) {
            String[] keys = key.split("|");
            if (keys.length < 6) {
                log.error("字段错误: " + key);
                continue;
            }
            String date = keys[0];
            String provinceId = keys[1];
            String channelCode = keys[2];
            String contentID = keys[3];
            String contentType = keys[4];
            String ruleID = keys[5];
            String totalFeeKey = date + "|" + provinceId + "|" + channelCode + "|"
                + contentID + "|" + contentType;
            double fee = totalFee.get(totalFeeKey);
            String abnormalFeeKey = totalFeeKey + "|" + ruleID;
            double abnFee = abnormalFee.get(abnormalFeeKey);
            if (checkExists(date, provinceId, contentID, contentType, channelCode, ruleID)) {
                this.updateDate(date, provinceId, contentID, contentType, channelCode, ruleID, abnFee, fee);
            } else {
                this.insertData(date, provinceId, contentID, contentType, channelCode, ruleID, abnFee, fee);
            }
        }
        abnormalFee.clear();
    }

    private boolean checkExists(String date, String provinceId, String contentID, String contentType,
                                String channelCode, String ruleId) {
        String checkExistsSql = "SELECT COUNT(*) recordTimes FROM " + StormConf.realTimeOutputTable
            + " WHERE RECORD_DAY=? AND PROVINCE_ID=? AND CONTENT_ID=?" +
            " AND SALE_PARM=? AND CONTENT_TYPE=? AND RULE_ID=?";
        try {
            if (conn == null) {
                conn = (new JDBCUtil()).getConnection();
            }
            PreparedStatement prepStmt = conn.prepareStatement(checkExistsSql);
            prepStmt.setString(1, date);
            prepStmt.setString(2, provinceId);
            prepStmt.setString(3, contentID);
            prepStmt.setString(4, channelCode);
            prepStmt.setString(5, contentType);
            prepStmt.setString(6, ruleId);

            ResultSet rs = prepStmt.executeQuery();
            rs.next();
            int count = rs.getInt("recordTimes");
            return count != 0;

        } catch (SQLException e) {
            log.error("查询sql错误" + checkExistsSql);
            e.printStackTrace();
        }
        return false;
    }

    private void insertData(String recordDay, String provinceId, String contentId, String contentType,
                            String channelCode, String ruleId, double abnormalFee, double totalFee) {
        if (!parameterId2ChannelIds.containsKey(channelCode)) {
            log.error("营销参数维表更新错误:" + new Date());
            return;
        }
        String[] chls = parameterId2ChannelIds.get(channelCode).split("|");
        String chl1 = chls[0];
        String chl2 = chls[1];
        String chl3 = chls[2];
        String insertDataSql = "INSERT INTO " + StormConf.realTimeOutputTable +
            "( RECORD_DAY,PROVINCE_ID,CHL1,CHL2,CHL3," +
            "  CONTENT_ID,SALE_PARM,ODR_ABN_FEE,ODR_FEE," +
            "  ABN_RAT,CONTENT_TYPE,RULE_ID) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
        double abnormalFeeRate = abnormalFee / totalFee;
        try {
            if (conn == null) {
                conn = (new JDBCUtil()).getConnection();
            }
            PreparedStatement prepStmt = conn.prepareStatement(insertDataSql);
            prepStmt.setString(1, recordDay);
            prepStmt.setString(2, provinceId);
            prepStmt.setString(3, chl1);
            prepStmt.setString(4, chl2);
            prepStmt.setString(5, chl3);
            prepStmt.setString(6, contentId);
            prepStmt.setString(7, channelCode);
            prepStmt.setDouble(8, abnormalFee);
            prepStmt.setDouble(9, totalFee);
            prepStmt.setDouble(10, abnormalFeeRate);
            prepStmt.setString(11, contentType);
            prepStmt.setInt(12, Integer.parseInt(ruleId));
            prepStmt.execute();
            prepStmt.execute("commit");
            if (StatisticsBolt.isDebug) {
                log.info("数据插入成功" + insertDataSql);
            }
        } catch (SQLException e) {
            log.error("插入sql错误" + insertDataSql);
            e.printStackTrace();
        }
    }

    private void updateDate(String date, String provinceId, String contentID, String contentType,
                            String channelCode, String ruleId, double abnormalFee, double totalFee) {
        String checkExistsSql = "SELECT ODR_ABN_FEE,ODR_FEE FROM " + StormConf.realTimeOutputTable
            + " WHERE RECORD_DAY=? AND PROVINCE_ID=? AND CONTENT_ID=?" +
            " AND SALE_PARM=? AND CONTENT_TYPE=? AND RULE_ID=?";

        try {
            if (conn == null) {
                conn = (new JDBCUtil()).getConnection();
            }
            PreparedStatement prepStmt = conn.prepareStatement(checkExistsSql);
            prepStmt.setString(1, date);
            prepStmt.setString(2, provinceId);
            prepStmt.setString(3, contentID);
            prepStmt.setString(4, channelCode);
            prepStmt.setString(5, contentType);
            prepStmt.setString(6, ruleId);

            double abnormalFeeOld = 0;
            double orderFeeOld = 0;
            ResultSet rs = prepStmt.executeQuery();
            while (rs.next()) {
                abnormalFeeOld = rs.getDouble("ODR_ABN_FEE");
                orderFeeOld = rs.getDouble("ODR_FEE");
            }
            double abnormalFeeNew = abnormalFeeOld + abnormalFee;
            double orderFeeNew = orderFeeOld + totalFee;
            double rateNew = abnormalFeeNew / orderFeeNew;

            String updateSql = " UPDATE " + StormConf.realTimeOutputTable
                + " SET ODR_ABN_FEE=\'" + abnormalFeeNew + "\', ODR_FEE=\'" + orderFeeNew + "\'," +
                "ABN_RAT=\'" + rateNew +
                " \' WHERE RECORD_DAY=? AND PROVINCE_ID=? AND CONTENT_ID=?" +
                " AND SALE_PARM=? AND CONTENT_TYPE=? AND RULE_ID=?";

            prepStmt = conn.prepareStatement(updateSql);
            prepStmt.setString(1, date);
            prepStmt.setString(2, provinceId);
            prepStmt.setString(3, contentID);
            prepStmt.setString(4, channelCode);
            prepStmt.setString(5, contentType);
            prepStmt.setString(6, ruleId);
            prepStmt.executeUpdate();
            prepStmt.execute("commit");
        } catch (SQLException e) {
            log.error("查询sql错误" + checkExistsSql);
            e.printStackTrace();
        }
    }
}

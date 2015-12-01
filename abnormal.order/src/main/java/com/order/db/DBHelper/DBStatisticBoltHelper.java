package com.order.db.DBHelper;

import com.order.db.JDBCUtil;
import com.order.util.LogUtil;
import com.order.util.StormConf;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;

/**
 * Created by LiMingji on 2015/6/4.
 */
public class DBStatisticBoltHelper implements Serializable {
    private static final long serialVersionUID = 1L;
    private static Logger log = Logger.getLogger(DBStatisticBoltHelper.class);
    private static transient Connection conn = null;

    private static Object LOCK = null;

    private static HashMap<String, String> parameterId2SecChannelId = null;
    private static HashMap<String, String> parameterId2ChannelIds = null;

    private static Connection getConn() throws SQLException {
        if (conn == null) {
            log.info("Connection is null!");
            conn = (new JDBCUtil()).getConnection();
        }
        return conn;
    }

    public DBStatisticBoltHelper() {
        LOCK = new Object();
        try {
            conn = getConn();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        synchronized (LOCK) {
            parameterId2SecChannelId = new HashMap<String, String>();
            parameterId2ChannelIds = new HashMap<String, String>();
        }
    }

    /**
     * 获取营销参数 二级渠道维表
     */
    public static void getData() {
        LogUtil.printLog("加载二维渠道维表" + new Date());
        if (LOCK == null) {
            LOCK = new Object();
        }
        synchronized (LOCK) {
            if (parameterId2ChannelIds == null) {
                parameterId2ChannelIds = new HashMap<String, String>();
            } else {
                parameterId2ChannelIds.clear();
            }
            if (parameterId2SecChannelId == null) {
                parameterId2SecChannelId = new HashMap<String, String>();
            } else {
                parameterId2SecChannelId.clear();
            }
        }
        String sql = "SELECT FIRST_CHANNEL_ID,SECOND_CHANNEL_ID,THIRD_CHANNEL_ID,PARAMETER_ID" +
            " FROM " + StormConf.channelCodesTable;
        try {
            if (conn == null) {
                conn = getConn();
            }
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            ResultSet resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                String firstChannelId = resultSet.getString("FIRST_CHANNEL_ID");
                String secondChannelId = resultSet.getString("SECOND_CHANNEL_ID");
                String thirdChannelId = resultSet.getString("THIRD_CHANNEL_ID");
                String parameterId = resultSet.getString("PARAMETER_ID");
                synchronized (LOCK) {
                    parameterId2SecChannelId.put(parameterId, secondChannelId);
                    parameterId2ChannelIds.put(parameterId, firstChannelId + "|" + secondChannelId + "|" + thirdChannelId);
                }
            }
        } catch (SQLException e) {
            log.error(sql + ":insert data to DB is failed.");
            e.printStackTrace();
        }
    }

    public static HashMap<String, String> getParameterId2SecChannelId() {
        return parameterId2SecChannelId;
    }

    public static HashMap<String, String> getParameterId2ChannelIds() {
        return parameterId2ChannelIds;
    }
}
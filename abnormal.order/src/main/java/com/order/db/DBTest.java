package com.order.db;

import com.order.util.StormConf;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBTest {

    /**
     * 如何在bolt中使用Oracle的JDBC.
     */
    public static void main(String[] args) throws SQLException {
        Connection conn = null;
        conn = (new JDBCUtil()).getConnection();
        String checkExistsSql = "SELECT COUNT(*) recordTimes FROM " + StormConf.realTimeOutputTable
            + " WHERE RECORD_DAY=? AND PROVINCE_ID=? AND CONTENT_ID=?" +
            " AND SALE_PARM=? AND CONTENT_TYPE=? AND RULE_ID=?";
        try {
            PreparedStatement prepStmt = conn.prepareStatement(checkExistsSql);
            prepStmt.setString(1, "20141105");
            prepStmt.setString(2, "000");
            prepStmt.setString(3, "123");
            prepStmt.setString(4, "00");
            prepStmt.setString(5, "1");
            prepStmt.setString(6, "1");

            ResultSet rs = prepStmt.executeQuery();
            rs.next();
            int count = rs.getInt("recordTimes");
            System.out.println(count);
        } catch (SQLException e) {
            System.out.println("查询sql错误" + checkExistsSql);
            e.printStackTrace();
        }
    }
}














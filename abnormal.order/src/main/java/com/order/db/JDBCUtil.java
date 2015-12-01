package com.order.db;

import oracle.jdbc.pool.OracleDataSource;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class JDBCUtil implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String CACHE_NAME = "MYCACHE";
    private OracleDataSource ods = null;
    static Logger log = Logger.getLogger(JDBCUtil.class);

    public JDBCUtil() {
        log.info("OracleDataSource Initialization");
        try {
            ods = new OracleDataSource();
            ods.setURL(DBConstant.DBURL);
            ods.setUser(DBConstant.DBUSER);
            ods.setPassword(DBConstant.DBPASSWORD);
            ods.setConnectionCacheName(CACHE_NAME);
            Properties cacheProps = new Properties();
            cacheProps.setProperty("MinLimit", "1");
            cacheProps.setProperty("MaxLimit", "1000");
            cacheProps.setProperty("InitialLimit", "5");
            cacheProps.setProperty("ConnectionWaitTimeout", "3");
            cacheProps.setProperty("ValidateConnection", "true");
            ods.setConnectionProperties(cacheProps);
        } catch (SQLException e) {
            log.error("JDBCUtil has an Exception");
        }
    }

    public Connection getConnection() throws SQLException {
        return getConnection("env.unspecified");
    }

    public Connection getConnection(String env) throws SQLException {
        if (ods == null) {
            ods = new OracleDataSource();
            log.error("OracleDataSource is null.This is an Exception.");
        }
        return ods.getConnection();
    }
}

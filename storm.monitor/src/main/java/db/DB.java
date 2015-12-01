package db;

import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DB implements java.io.Serializable {

    private static final long serialVersionUID = 1L;
    public Map<String, String> parameters;
    transient Connection conn;
    private static Logger log = Logger.getLogger(DB.class);
    public static final String filePath = "table.properties";

    public DB() {
        this.parameters = readProperties(filePath);
        log.info("DB filePath: " + filePath);
    }

    private Map<String, String> readProperties(String filePath) {
        Properties props = new Properties();
        Map<String, String> parameters = new HashMap<String, String>();
        InputStream in;
        in = DB.class.getResourceAsStream("/" + filePath);
        try {
            if (new java.io.File(filePath).exists()) {
                in = new BufferedInputStream(new FileInputStream(filePath));
            }
            props.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Enumeration<?> en = props.propertyNames();
        while (en.hasMoreElements()) {
            String key = (String) en.nextElement();
            String property = props.getProperty(key);
            parameters.put(key, property);
            log.info("table:" + key + "=" + property);
        }
        return parameters;
    }

    // insert 2 fields
    public void insertUser(String tableName, String field1, String field2) {
        String fields = parameters.get(tableName);
        Connection conn;
        String strSQL = "";
        try {
            conn = getConn();
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            strSQL = "insert into " + tableName + "(" + fields + ") values('"
                + field1 + "','" + field2 + "')";
            log.info(strSQL);
            stmt.execute(strSQL);
            stmt.execute("commit");
            stmt.close();
            // conn.close();
        } catch (SQLException e) {
            log.error(strSQL + ":insert data to DB is failed.");
            e.printStackTrace();
        }
    }

    private Connection getConn() throws SQLException {
        if (conn == null) {
            log.info("Connection is null!");
            conn = (new JDBCUtil()).getConnection();
        }
        return conn;
    }

    // insert 3 fields
    public void insertUser(String tableName, String field1, String field2,
                           String field3) {
        String fields = parameters.get(tableName);
        String strSQL = "";
        try {
            conn = getConn();
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            strSQL = "insert into " + tableName + "(" + fields + ") values('"
                + field1 + "','" + field2 + "','" + field3 + "')";
            log.info(strSQL);
            stmt.execute(strSQL);
            stmt.execute("commit");
            stmt.close();
            // conn.close();
        } catch (SQLException e) {
            log.error(strSQL + ":insert data to DB is failed.");
            e.printStackTrace();
        }
    }
}

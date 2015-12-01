package db;

public class DBConstant {
    public static final String DBDRIVER = "oracle.jdbc.driver.OracleDriver";
    public static final String DBURL = "jdbc:oracle:thin:@(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST = 192.168.3.183)(PORT = 1521))(ADDRESS = (PROTOCOL = TCP)(HOST = 192.168.3.185)(PORT = 1521))(LOAD_BALANCE = yes)(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = service_ora)))";
    public static final String DBUSER = "unidata";
    public static final String DBPASSWORD = "3edc%RDX";

    // the lab's oracle site and password is as follows.
  /*
	 * public static final String DBDRIVER = "oracle.jdbc.driver.OracleDriver";
	 * public static final String DBURL =
	 * "jdbc:oracle:thin:@10.1.69.173:1521:ORCLBI"; public static final String
	 * DBUSER = "huangq"; public static final String DBPASSWORD = "123456";
	 */

}
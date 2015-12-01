package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import db.DB;
import db.DBConstant;
import org.apache.log4j.Logger;
import util.FName;
import util.InfoUpdate;
import util.StreamId;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class BkPtDB extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    private static Map<String, String> itemInfo = new HashMap<String, String>();
    private Map<String, Integer> itemCount = new HashMap<String, Integer>();
    private String tableName;
    private DB db = new DB();
    private String path;
    static Logger log = Logger.getLogger(BkPtDB.class);

    public BkPtDB(String tableName, String path) {
        this.tableName = tableName;
        this.path = path;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        log.info("Initializing the class BkptDB.java.the bookInfo's path is :"
            + path);
        InfoUpdate.updateBookInfo(path, itemInfo);
    }

    public void execute(Tuple input) {
        try {
            String itemID = input.getStringByField(FName.ITEM_ID.name());
            Integer count = input.getIntegerByField(FName.COUNT.name());
            itemCount.put(itemID, count);
        } catch (IllegalArgumentException e) {
            String strID = input.getSourceStreamId();
            if (strID.equals(StreamId.SIGNALDB.name())) {
                String timePeriod = input.getStringByField(FName.ACTION.name());
                downloadToDB(timePeriod);
            } else if (strID.equals(StreamId.SIGNALUPDATE.name())) {
                InfoUpdate.updateBookInfo(path, itemInfo);
            } else if (strID.equals(StreamId.SIGNAL24H.name())) {
                log.info("24Hour is coming.");
                itemCount.clear();
            }
        }
        collector.ack(input);
    }

    private void downloadToDB(String timePeriod) {
        log.info("start to write to DB!");
        int infosize = itemInfo.size();
        if (infosize == 0) {
            InfoUpdate.updateBookInfo(path, itemInfo);
        }
        log.info("bookinfo's size: " + infosize);
        Map<String, String> parameters = db.parameters;
        String fields = parameters.get(tableName);
        try {
            long startTime = System.currentTimeMillis();
            String sql = "insert into " + this.tableName + "(" + fields + ")"
                + " values(?,?,?,?)";
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection con = DriverManager.getConnection(DBConstant.DBURL,
                DBConstant.DBUSER, DBConstant.DBPASSWORD);
            con.setAutoCommit(false);
            PreparedStatement pst = con.prepareStatement(sql);
            for (Map.Entry<String, Integer> entry : itemCount.entrySet()) {
                String itemid = entry.getKey();
                Integer count = entry.getValue();
                String type = itemInfo.get(itemid);
                pst.setString(1, timePeriod);
                pst.setString(2, itemid);
                pst.setString(3, type);
                pst.setInt(4, count);
                pst.addBatch();
            }
            pst.executeBatch();
            con.commit();
            pst.close();
            con.close();
            long endTime = System.currentTimeMillis();
            log.info("the patch insert taked time is ：" + (endTime - startTime)
                + "ms");
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("insert data to DB is failed.");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            log.error("the class is Not Found!");
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public void cleanup() {
    }
}

package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import db.DB;
import db.DBConstant;
import main.StormConf;
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

public class BookVisitDB extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Map<String, Integer> bookVisit = new HashMap<String, Integer>();
    private static Map<String, String> itemInfo = new HashMap<String, String>();
    private String tableName;
    private DB db = new DB();
    private String path;
    static Logger log = Logger.getLogger(BookVisitDB.class);

    public BookVisitDB(String tableName) {
        this.tableName = tableName;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.path = stormConf.get(StormConf.BOOKINFO).toString();
        log.info("Initializing the class BookVisitDB.java. The bookInfo's path is :"
            + path);
        InfoUpdate.updateChapterInfo(path, itemInfo);
    }

    public void execute(Tuple input) {
        try {
            String book_id = input.getStringByField(FName.BOOK_ID.name());
            Integer count = input.getIntegerByField(FName.COUNT.name());
            bookVisit.put(book_id, count);
        } catch (IllegalArgumentException e) {
            String strID = input.getSourceStreamId();
            if (strID.equals(StreamId.SIGNALDB.name())) {
                String timePeriod = input.getStringByField(FName.ACTION.name()).trim();
                downloadToDB(timePeriod);
            } else if (strID.equals(StreamId.SIGNALUPDATE.name())) {
                InfoUpdate.updateChapterInfo(path, itemInfo);
            } else if (strID.equals(StreamId.SIGNAL24H.name())) {
                log.info("24Hour is coming.");
                bookVisit.clear();
            }
        }
        collector.ack(input);
    }

    private void downloadToDB(final String timePeriod) {
        log.info("start to write to DB!");
        int infosize = itemInfo.size();
        if (infosize == 0) {
            InfoUpdate.updateChapterInfo(path, itemInfo);
        }
        log.info("itemInfo size: " + infosize);
        Map<String, String> parameters = db.parameters;
        String fields = parameters.get(tableName);
        try {
            long startTime = System.currentTimeMillis();
            String sql = "insert into " + this.tableName + "(" + fields + ")"
                + " values(?,?,?,?,?)";
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection con = DriverManager.getConnection(DBConstant.DBURL,
                DBConstant.DBUSER, DBConstant.DBPASSWORD);
            con.setAutoCommit(false);
            PreparedStatement pst = con.prepareStatement(sql);
            for (Map.Entry<String, Integer> entry : bookVisit.entrySet()) {
                String book_id = entry.getKey();
                Integer count = entry.getValue();
                String com = itemInfo.get(book_id);
                String type_id = "";
                String class_id = "";
                if (com != null) {
                    String[] words = com.split("\\|", -1);
                    type_id = words[0];
                    class_id = words[1];
                }
                pst.setString(1, timePeriod);
                pst.setString(2, book_id);
                pst.setString(3, type_id);
                pst.setString(4, class_id);
                pst.setInt(5, count);
                pst.addBatch();
            }
            pst.executeBatch();
            con.commit();
            pst.close();
            con.close();
            long endTime = System.currentTimeMillis();
            log.info("BookVisitDB：" + (endTime - startTime) + "ms");
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

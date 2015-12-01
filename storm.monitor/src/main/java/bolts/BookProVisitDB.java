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

public class BookProVisitDB extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Map<String, Integer> bookProVisit = new HashMap<String, Integer>();
    private static Map<String, String> itemInfo = new HashMap<String, String>();
    private String tableName;
    private DB db = new DB();
    private String path;
    static Logger log = Logger.getLogger(BookProVisitDB.class);

    public BookProVisitDB(String tableName) {
        this.tableName = tableName;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.path = stormConf.get(StormConf.BOOKINFO).toString();
        InfoUpdate.updateChapterInfo(path, itemInfo);
    }

    public void execute(Tuple input) {
        try {
            String book_pro = input.getStringByField(FName.BOOK_PRO.name());
            Integer count = input.getIntegerByField(FName.COUNT.name());
            bookProVisit.put(book_pro, count);
        } catch (IllegalArgumentException e) {
            String strID = input.getSourceStreamId();
            if (strID.equals(StreamId.SIGNALDB.name())) {
                String timePeriod = input.getStringByField(FName.ACTION.name()).trim();
                downloadToDB(timePeriod);
            } else if (strID.equals(StreamId.SIGNALUPDATE.name())) {
                InfoUpdate.updateChapterInfo(path, itemInfo);
            } else if (strID.equals(StreamId.SIGNAL24H.name())) {
                log.info("24Hour is coming.");
                bookProVisit.clear();
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
        log.info("itemInfo size: " + itemInfo.size());
        Map<String, String> parameters = db.parameters;
        String fields = parameters.get(tableName);
        try {
            long startTime = System.currentTimeMillis();
            String sql = "insert into " + this.tableName + "(" + fields + ")"
                + " values(?,?,?,?,?,?)";
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection con = DriverManager.getConnection(DBConstant.DBURL,
                DBConstant.DBUSER, DBConstant.DBPASSWORD);
            con.setAutoCommit(false);
            PreparedStatement pst = con.prepareStatement(sql);
            for (Map.Entry<String, Integer> entry : bookProVisit.entrySet()) {
                String[] book_pro = entry.getKey().split("\\|", -1);
                String book_id = book_pro[0];
                String province_id = book_pro[1];
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
                pst.setString(2, province_id);
                pst.setString(3, book_id);
                pst.setString(4, type_id);
                pst.setString(5, class_id);
                pst.setInt(6, count);
                pst.addBatch();
            }
            pst.executeBatch();
            con.commit();
            pst.close();
            con.close();
            long endTime = System.currentTimeMillis();
            log.info("BookProVisitDB: " + (endTime - startTime) + "ms");
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

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
import util.StreamId;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class OrdProPlatUVDB extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Map<String, Integer> proplatUv = new HashMap<String, Integer>();
    private final String tableName;
    private DB db = new DB();
    static Logger log = Logger.getLogger(OrdProPlatUVDB.class);

    public OrdProPlatUVDB(String tableName) {
        this.tableName = tableName;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            String proplatid = input.getStringByField(FName.PROVINCE_ID.name());
            Integer item = input.getIntegerByField(FName.COUNT.name());
            proplatUv.put(proplatid, item);
        } catch (IllegalArgumentException e) {
            String stmId = input.getSourceStreamId();
            if (stmId.equals(StreamId.SIGNALDB.name())) {
                String timePeriod = input.getStringByField(FName.ACTION.name())
                    .trim();
                downloadToDB(timePeriod);
            } else if (stmId.equals(StreamId.SIGNAL24H.name())) {
                log.info("24Hour is coming.");
                proplatUv.clear();
            }
        }
        collector.ack(input);
    }

    private void downloadToDB(String timePeriod) {
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
            for (Map.Entry<String, Integer> entry : proplatUv.entrySet()) {
                String key = entry.getKey();
                String[] words = key.split("\\|", -1);
                pst.setString(1, timePeriod);
                pst.setString(2, words[0]);
                pst.setString(3, words[1]);
                pst.setInt(4, entry.getValue());
                pst.addBatch();
            }
            pst.executeBatch();
            con.commit();
            pst.close();
            con.close();
            long endTime = System.currentTimeMillis();
            log.info("OrdProPlatUVDB：" + (endTime - startTime) + "ms");
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

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
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

public class WapPVSuccessRate extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    private Map<String, Integer> PVSum = new HashMap<String, Integer>();
    private Map<String, Integer> PVSuccess = new HashMap<String, Integer>();
    private DecimalFormat dformat = new DecimalFormat("0.00000");
    private String tableName;
    private DB db = new DB();
    static Logger log = Logger.getLogger(WapPVSuccessRate.class);

    public WapPVSuccessRate(String tableName) {
        this.tableName = tableName;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String pageName = null;
        try {
            pageName = input.getStringByField(FName.PAGENAME.name());
        } catch (IllegalArgumentException e) {
        }
        if (pageName != null) {
            String beartype = "";// set beartype to ""
            String errorCode = input.getStringByField(FName.ERRCODE.name());
            String key = pageName + "|" + beartype;
            pvAdd(key, PVSum);
            int error = 0;
            if (!errorCode.equalsIgnoreCase("")) {
                error = Integer.valueOf(errorCode);
            }
            if (error != 500) {
                pvAdd(key, PVSuccess);
            }
        } else {
            if (input.getSourceStreamId().equals(StreamId.SIGNAL15MIN.name())) {
                pageName = input.getStringByField(FName.ACTION15MIN.name());
                String timePeriod = pageName.trim();
                pvSuccessRateCount(timePeriod);
                clearData();
            }
        }
        collector.ack(input);
    }

    private void pvSuccessRateCount(String timePeriod) {
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
            for (Map.Entry<String, Integer> entry : PVSuccess.entrySet()) {
                String key = entry.getKey();
                Double rate = (double) entry.getValue() / PVSum.get(key);
                String rateStr = dformat.format(rate);
                String[] words = key.split("\\|", -1);
                pst.setString(1, timePeriod);
                pst.setString(2, words[0]);
                pst.setString(3, words[1]);
                pst.setString(4, rateStr);
                pst.addBatch();
            }
            pst.executeBatch();
            con.commit();
            pst.close();
            con.close();
            long endTime = System.currentTimeMillis();
            log.info("WapPVSuccessRateBolt：" + (endTime - startTime) + "ms");
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("insert data to DB is failed.");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            log.error("the class is Not Found!");
        }
    }

    private void pvAdd(String key, Map<String, Integer> PVMap) {
        Integer PVcount = PVMap.get(key);
        if (PVcount == null)
            PVcount = 0;
        PVcount++;
        PVMap.put(key, PVcount);
    }

    private void clearData() {
        PVSuccess.clear();
        PVSum.clear();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public void cleanup() {
    }
}

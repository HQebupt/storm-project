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

public class PVRate extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    private Map<String, Integer> PVItem = new HashMap<String, Integer>();
    private DecimalFormat dformat = new DecimalFormat("0.00000");
    private static int PVsum = 0;
    private String tableName;
    private DB db = new DB();
    static Logger log = Logger.getLogger(PVRate.class);

    public PVRate(String tableName) {
        this.tableName = tableName;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String data = null;
        try {
            data = input.getStringByField(FName.BEARTYPE.name());
        } catch (IllegalArgumentException e) {
        }
        if (data != null) {
            String itemPart = input.getString(0);// this index is important and
            // volatile,Be careful.
            PVsum++;
            data = build(data.trim());
            String key = itemPart + "|" + data;
            Integer PVcount = pvCount(key);
            PVItem.put(key, PVcount);
        } else {
            if (input.getSourceStreamId().equals(StreamId.SIGNAL15MIN.name())) {
                data = input.getStringByField(FName.ACTION15MIN.name());
                String timePeriod = data.trim();
                pvRateCount(timePeriod);
                clearData();
            }
        }
        collector.ack(input);
    }

    private String build(String data) {
        if (data.equalsIgnoreCase("2"))
            return "2";
        else
            return "3";
    }

    private Integer pvCount(String key) {
        Integer PVcount = PVItem.get(key);
        if (PVcount == null)
            PVcount = 0;
        PVcount++;
        return PVcount;
    }

    private void pvRateCount(String timePeriod) {
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
            for (Map.Entry<String, Integer> entry : PVItem.entrySet()) {
                String[] words = entry.getKey().split("\\|", -1);
                if (PVsum != 0) {
                    Double rate = (double) entry.getValue() / PVsum;
                    String rateStr = dformat.format(rate);
                    int err = words[0].indexOf('?');
                    if (err == -1) {
                        pst.setString(1, timePeriod);
                        pst.setString(2, words[0]);
                        pst.setString(3, words[1]);
                        pst.setString(4, rateStr);
                        pst.addBatch();
                    } else {
                        log.info("this is error code page. This pvRate is abandoned："
                            + words[0]);
                    }
                }
            }
            pst.executeBatch();
            con.commit();
            pst.close();
            con.close();
            long endTime = System.currentTimeMillis();
            log.info("PVRateBolt：" + (endTime - startTime) + "ms");
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("insert data to DB is failed.");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            log.error("the class is Not Found!");
        }
    }

    private void clearData() {
        PVItem.clear();
        PVsum = 0;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public void cleanup() {
    }
}

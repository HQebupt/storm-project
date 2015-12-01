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

public class DelayAverage extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    private Map<String, Integer> delaySum = new HashMap<String, Integer>();
    private Map<String, Integer> delayNumberCount = new HashMap<String, Integer>();
    private DecimalFormat dformat = new DecimalFormat("0.00");
    private String tableName;
    private DB db = new DB();
    static Logger log = Logger.getLogger(DelayAverage.class);

    public DelayAverage(String tableName) {
        this.tableName = tableName;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String beartype = null;
        try {
            beartype = input.getStringByField(FName.BEARTYPE.name());
        } catch (IllegalArgumentException e) {
        }
        if (beartype != null) {
            String pageName = input.getString(0); // the index is important.
            String deyStr = input.getString(1); // the index is important.
            int delay = 0;
            if (!deyStr.equalsIgnoreCase("")) {
                delay = Integer.valueOf(deyStr);
            }
            beartype = build(beartype.trim());
            String key = pageName + "|" + beartype;
            sumDelay(key, delay, delaySum);
            countDelay(key, delayNumberCount);
        } else if (input.getSourceStreamId()
            .equals(StreamId.SIGNAL15MIN.name())) {
            String timePeriod = input
                .getStringByField(FName.ACTION15MIN.name());
            delayAverageCalculate(timePeriod);
            clearData();
        }
        collector.ack(input);
    }

    private String build(String beartype) {
        if (beartype.equalsIgnoreCase("2"))
            return "2";
        else
            return "3";
    }

    private void countDelay(String key, Map<String, Integer> delayCount) {
        int countNum = getDelay(key, delayNumberCount);
        countNum++;
        delayCount.put(key, countNum);
    }

    private void sumDelay(String key, int delay, Map<String, Integer> delaySum) {
        int delayCurrentSum = getDelay(key, delaySum);
        delayCurrentSum += delay;
        delaySum.put(key, delayCurrentSum);
    }

    private void delayAverageCalculate(String timePeriod) {
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
            for (Map.Entry<String, Integer> entry : delaySum.entrySet()) {
                String key = entry.getKey();
                int sum = entry.getValue();
                int numberSum = delayNumberCount.get(key);
                Double delayAverage = (double) sum / numberSum;
                String delayStr = dformat.format(delayAverage);
                String[] words = key.split("\\|", -1);
                pst.setString(1, timePeriod);
                pst.setString(2, words[0]);
                pst.setString(3, words[1]);
                pst.setString(4, delayStr);
                pst.addBatch();
            }
            pst.executeBatch();
            con.commit();
            pst.close();
            con.close();
            long endTime = System.currentTimeMillis();
            log.info("DelayAverageBolt：" + (endTime - startTime) + "ms");
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("insert data to DB is failed.");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            log.error("the class is Not Found!");
        }
    }

    private int getDelay(String key, Map<String, Integer> delayMap) {
        Integer delay = delayMap.get(key);
        if (delay == null)
            delay = 0;
        return delay;
    }

    private void clearData() {
        delayNumberCount.clear();
        delaySum.clear();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public void cleanup() {
    }
}
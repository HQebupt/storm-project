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
import util.TimeConst;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class InterfacePVSuccessRate extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    private Map<String, Integer> PVTotal = new HashMap<String, Integer>();
    private Map<String, Integer> PVFail = new HashMap<String, Integer>();
    private DecimalFormat dformat = new DecimalFormat("0.000");
    private String tableName;
    private DB db = new DB();
    private Set<String> errCode = new HashSet<String>();
    static Logger log = Logger.getLogger(InterfacePVSuccessRate.class);

    public InterfacePVSuccessRate(String tableName) {
        this.tableName = tableName;
        creatErrCode();
    }

    private void creatErrCode() {
        for (String err : TimeConst.ERRCODE) {
            errCode.add(err);
        }
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
            String pageName = input.getStringByField(FName.ACTION.name());
            String rscode = input.getStringByField(FName.RESULTCODE.name())
                .trim();
            beartype = build(beartype.trim());
            String key = pageName + "|" + beartype;
            addPv(key, PVTotal);
            if (errCode.contains(rscode)) {
                addPv(key, PVFail);
            }
        } else if (input.getSourceStreamId()
            .equals(StreamId.SIGNAL15MIN.name())) {
            String timePeriod = input
                .getStringByField(FName.ACTION15MIN.name());
            pvSuccessRateCount(timePeriod);
            clearData();
        }
        collector.ack(input);
    }

    private void addPv(String key, Map<String, Integer> pvMap) {
        Integer count = pvMap.get(key);
        if (count == null) {
            count = 0;
        }
        count++;
        pvMap.put(key, count);

    }

    private String build(String beartype) {
        if (beartype.equalsIgnoreCase("2"))
            return "2";
        else
            return "3";
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
            for (Map.Entry<String, Integer> entry : PVTotal.entrySet()) {
                String key = entry.getKey();
                int pvTotalValue = entry.getValue();
                int pvFailValue = getPV(key, PVFail);
                Double rate = (double) (pvTotalValue - pvFailValue)
                    / pvTotalValue;
                String rateStr = dformat.format(rate);
                String[] words = key.split("\\|", -1);
                log.info("SOftPVSUCCESS:" + words[0] + " " + words[1] + " "
                    + rateStr);
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
            log.info("InterfacePVSuccessRateBolt：" + (endTime - startTime)
                + "ms");
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("insert data to DB is failed.");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            log.error("the class is Not Found!");
        }
    }

    private int getPV(String key, Map<String, Integer> PVMap) {
        Integer PVcount = PVMap.get(key);
        if (PVcount == null)
            PVcount = 0;
        return PVcount;
    }

    private void clearData() {
        PVFail.clear();
        PVTotal.clear();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public void cleanup() {
    }
}

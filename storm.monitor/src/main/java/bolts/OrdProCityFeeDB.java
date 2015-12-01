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

public class OrdProCityFeeDB extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private String tableName;
    private DB db = new DB();
    OutputCollector collector;
    Map<String, Integer> proCityFee = new HashMap<String, Integer>();
    static Logger log = Logger.getLogger(OrdProCityFeeDB.class);

    public OrdProCityFeeDB(String tableName) {
        this.tableName = tableName;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            String realInforFee = input.getStringByField(FName.REALINFORFEE
                .name());
            String proCity = input.getStringByField(FName.PROCITY.name());
            calculateFee(proCity, realInforFee);
        } catch (IllegalArgumentException e) {
            String strID = input.getSourceStreamId();
            if (strID.equals(StreamId.SIGNALDB.name())) {
                String timePeriod = input.getStringByField(FName.ACTION.name())
                    .trim();
                downloadToDB(timePeriod);
            } else if (strID.equals(StreamId.SIGNAL24H.name())) {
                log.info("24Hour is coming.");
                proCityFee.clear();
            }
        }
        collector.ack(input);
    }

    private void downloadToDB(String timePeriod) {
        log.info("start to write to DB!");
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
            for (Map.Entry<String, Integer> entry : proCityFee.entrySet()) {
                String proCity = entry.getKey();
                Integer fee = entry.getValue();
                String[] words = proCity.split("\\|", -1);
                // log.info("the keyProCity is "+proCity+" . ");
                pst.setString(1, timePeriod);
                pst.setString(2, words[0]);
                pst.setString(3, words[1]);
                pst.setInt(4, fee);
                pst.addBatch();
            }
            pst.executeBatch();
            con.commit();
            pst.close();
            con.close();
            long endTime = System.currentTimeMillis();
            log.info("the patch insert taked time is : "
                + (endTime - startTime) + " ms");
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("insert data to DB is failed.");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            log.error("the class is Not Found!");
        }
    }

    private void calculateFee(String province_id, String realInforFee) {
        int inforFee = 0;
        if (!realInforFee.equalsIgnoreCase("")) {
            inforFee = Integer.valueOf(realInforFee);
        }
        int inforFeeCurrent = calculateInforFee(province_id, inforFee,
            proCityFee);
        proCityFee.put(province_id, inforFeeCurrent);
    }

    private int calculateInforFee(String province_id, int inforFee,
                                  Map<String, Integer> infoFeeMap) {
        Integer inforFeeCurrent = infoFeeMap.get(province_id);
        if (inforFeeCurrent == null) {
            inforFeeCurrent = 0;
        }
        inforFeeCurrent += inforFee;
        return inforFeeCurrent;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public void cleanup() {
    }
}

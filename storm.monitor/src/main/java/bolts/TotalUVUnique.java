package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import db.DB;
import org.apache.log4j.Logger;
import util.FName;
import util.StreamId;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TotalUVUnique extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Set<String> userSet = new HashSet<String>();
    Set<String> ipSet = new HashSet<String>();
    private String tableName;
    private DB db = new DB();
    static Logger log = Logger.getLogger(TotalUVUnique.class);

    public TotalUVUnique(String tableName) {
        this.tableName = tableName;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            String msisdn = input.getStringByField(FName.MSISDN.name());
            if (msisdn.length() >= 1) {
                String firstNum = msisdn.substring(0, 1);
                Boolean isvisited;
                if (!firstNum.equalsIgnoreCase("9")) {
                    isvisited = isVisted(msisdn);
                    if (!isvisited) {
                        userSet.add(msisdn);
                    }
                } else {
                    String remoteip = input.getStringByField(FName.REMOTEIP
                        .name());
                    isvisited = isIPVisted(remoteip);
                    if (!isvisited) {
                        ipSet.add(remoteip);
                    }
                }
            }
        } catch (IllegalArgumentException e) {
            if (input.getSourceStreamId().equals(StreamId.SIGNALUNIQUE.name())) {
                log.info("1Hour is coming.");
                String timePeriod = input.getStringByField(FName.ACTION.name());
                int count = ipSet.size() + userSet.size();
                log.info("timePeriod:" + timePeriod);
                downloadToDB(timePeriod, count);
                clearData();
            }
        }
        collector.ack(input);
    }

    private void downloadToDB(String timePeriod, Integer total) {
        db.insertUser(tableName, timePeriod, total.toString());
    }

    private void clearData() {
        userSet.clear();
        ipSet.clear();
    }

    private Boolean isIPVisted(String remoteip) {
        return ipSet.contains(remoteip);
    }

    private Boolean isVisted(String msisdn) {
        return userSet.contains(msisdn);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public void cleanup() {
    }
}

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

public class SoftUVUnique extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Set<String> userSet = new HashSet<String>();
    private String tableName;
    private DB db = new DB();
    static Logger log = Logger.getLogger(SoftUVUnique.class);

    public SoftUVUnique(String tableName) {
        this.tableName = tableName;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            String msisdn = input.getStringByField(FName.MSISDN.name());
            Boolean isvisited = isVisted(msisdn);
            if (!isvisited) {
                userSet.add(msisdn);
            }
        } catch (IllegalArgumentException e) {
            if (input.getSourceStreamId().equals(StreamId.SIGNALUNIQUE.name())) {
                log.info("1Hour is coming.");
                String timePeriod = input.getStringByField(FName.ACTION.name());
                log.info("timePeriod:" + timePeriod);
                downloadToDB(timePeriod, userSet.size());
                userSet.clear();
            }
        }
        collector.ack(input);
    }

    private void downloadToDB(String timePeriod, Integer total) {
        db.insertUser(tableName, timePeriod, total.toString());
    }

    private Boolean isVisted(String msisdn) {
        return userSet.contains(msisdn);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}

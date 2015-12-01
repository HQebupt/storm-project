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

import java.util.Map;

public class OrdUVUnique extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    private String tableName;
    private DB db = new DB();
    static Integer total = 0;
    static Logger log = Logger.getLogger(OrdUVUnique.class);

    public OrdUVUnique(String tableName) {
        this.tableName = tableName;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            String msisdn = input.getStringByField(FName.MSISDN.name());
            total++;
        } catch (IllegalArgumentException e) {
            if (input.getSourceStreamId().equals(StreamId.SIGNALUNIQUE.name())) {
                log.info("1Hour is coming.");
                String timePeriod = input.getStringByField(FName.ACTION.name());
                log.info("timePeriod:" + timePeriod);
                downloadToDB(timePeriod, total);
                total = 0;
            }
        }
        collector.ack(input);
    }

    private void downloadToDB(String timePeriod, Integer total) {
        db.insertUser(tableName, timePeriod, total.toString());
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public void cleanup() {
    }
}

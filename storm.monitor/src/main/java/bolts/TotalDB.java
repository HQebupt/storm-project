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

public class TotalDB extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    static Integer total = 0;
    private String tableName;
    private DB db = new DB();
    static Logger log = Logger.getLogger(TotalDB.class);

    public TotalDB(String tableName) {
        this.tableName = tableName;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        Integer totalComing = null;
        try {
            totalComing = input.getIntegerByField(FName.TOTAL.name());
        } catch (IllegalArgumentException e) {
        }
        if (totalComing != null) {
            total = totalComing;
        } else {
            if (input.getSourceStreamId().equals(StreamId.SIGNALDB.name())) {
                String action = input.getStringByField(FName.ACTION.name());
                String timePeriod = action.trim();
                downloadToDB(timePeriod);
            } else if (input.getSourceStreamId().equals(StreamId.SIGNAL24H.name())) {
                log.info("24Hour is coming.");
                total = 0;
            }
        }
        collector.ack(input);
    }

    private void downloadToDB(String timePeriod) {
        db.insertUser(tableName, timePeriod, total.toString());
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public void cleanup() {
    }
}

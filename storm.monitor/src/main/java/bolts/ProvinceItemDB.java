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

import java.util.HashMap;
import java.util.Map;

public class ProvinceItemDB extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Map<String, Integer> provincePV = new HashMap<String, Integer>();
    private String tableName;
    private DB db = new DB();
    static Logger log = Logger.getLogger(ProvinceItemDB.class);

    public ProvinceItemDB(String tableName) {
        this.tableName = tableName;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String province_id = null;
        try {
            province_id = input.getStringByField(FName.PROVINCE_ID.name());
        } catch (IllegalArgumentException e) {
        }
        if (province_id != null) {
            Integer item = input.getIntegerByField(FName.COUNT.name());
            provincePV.put(province_id, item);
        } else {
            if (input.getSourceStreamId().equals(StreamId.SIGNALDB.name())) {
                province_id = input.getStringByField(FName.ACTION.name());
                String timePeriod = province_id.trim();
                downloadToDB(timePeriod);
            } else if (input.getSourceStreamId().equals(
                StreamId.SIGNAL24H.name())) {
                log.info("24Hour is coming.");
                provincePV.clear();
            }
        }
        collector.ack(input);
    }

    private void downloadToDB(String timePeriod) {
        for (Map.Entry<String, Integer> entry : provincePV.entrySet()) {
            db.insertUser(tableName, timePeriod, entry.getKey(), entry
                .getValue().toString());
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public void cleanup() {
    }
}

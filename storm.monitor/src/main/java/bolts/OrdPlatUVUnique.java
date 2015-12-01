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

public class OrdPlatUVUnique extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Map<String, Integer> platformItem = new HashMap<String, Integer>();
    private String tableName;
    private DB db = new DB();
    static Logger log = Logger.getLogger(OrdPlatUVUnique.class);

    public OrdPlatUVUnique(String tableName) {
        this.tableName = tableName;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            String platform = input.getStringByField(FName.PLATFORM.name());
            Integer itemSum = itemCount(platform);
            platformItem.put(platform, itemSum);
        } catch (IllegalArgumentException e) {
            if (input.getSourceStreamId().equals(StreamId.SIGNALUNIQUE.name())) {
                System.out.println("1Hour is coming.");
                String timePeriod = input.getStringByField(FName.ACTION.name());
                log.info("timePeriod:" + timePeriod);
                downloadToDB(timePeriod);
                platformItem.clear();
            }
        }
        collector.ack(input);
    }

    private void downloadToDB(String timePeriod) {
        for (Map.Entry<String, Integer> entry : platformItem.entrySet()) {
            db.insertUser(tableName, timePeriod, entry.getKey(), entry
                .getValue().toString());
        }
    }

    private Integer itemCount(String platform) {
        int count = getItemCount(platform);
        count++;
        return count;
    }

    private int getItemCount(String platform) {
        Integer count = platformItem.get(platform);
        if (count == null) {
            count = 0;
        }
        return count;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}

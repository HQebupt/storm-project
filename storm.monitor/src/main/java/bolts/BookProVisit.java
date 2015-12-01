package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import util.FName;
import util.StreamId;

import java.util.HashMap;
import java.util.Map;

public class BookProVisit extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Map<String, Integer> bookProItem = new HashMap<String, Integer>();
    static Logger log = Logger.getLogger(BookProVisit.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            String book_id = input.getStringByField(FName.BOOK_ID.name());
            String province_id = input.getStringByField(FName.PROVINCE_ID.name());
            String key = buildkey(book_id, province_id);
            Integer itemSum = itemCount(key);
            bookProItem.put(key, itemSum);
            collector.emit(new Values(key, itemSum));
        } catch (IllegalArgumentException e) {
            if (input.getSourceStreamId().equals(StreamId.SIGNAL24H.name())) {
                log.info("24Hour is coming.");
                bookProItem.clear();
            }
        }
        collector.ack(input);
    }

    private String buildkey(String book_id, String province_id) {
        return book_id + "|" + province_id;
    }

    private int itemCount(String province_id) {
        Integer count = getItemCount(province_id);
        count++;
        return count;
    }

    private int getItemCount(String province_id) {
        Integer count = bookProItem.get(province_id);
        if (count == null) {
            count = 0;
        }
        return count;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FName.BOOK_PRO.name(), FName.COUNT.name()));
    }

    public void cleanup() {
    }
}

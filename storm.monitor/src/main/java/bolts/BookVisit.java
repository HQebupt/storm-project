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

public class BookVisit extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Map<String, Integer> bookItem = new HashMap<String, Integer>();
    static Logger log = Logger.getLogger(BookVisit.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            String book_id = input.getStringByField(FName.BOOK_ID.name());
            Integer itemSum = itemCount(book_id);
            bookItem.put(book_id, itemSum);
            collector.emit(new Values(book_id, itemSum));
        } catch (IllegalArgumentException e) {
            if (input.getSourceStreamId().equals(StreamId.SIGNAL24H.name())) {
                log.info("24Hour is coming.");
                bookItem.clear();
            }
        }
        collector.ack(input);
    }

    private int itemCount(String province_id) {
        Integer count = getItemCount(province_id);
        count++;
        return count;
    }

    private int getItemCount(String province_id) {
        Integer count = bookItem.get(province_id);
        if (count == null) {
            count = 0;
        }
        return count;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FName.BOOK_ID.name(), FName.COUNT.name()));
    }

    public void cleanup() {
    }
}

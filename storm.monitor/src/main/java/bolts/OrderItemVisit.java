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

public class OrderItemVisit extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    private Map<String, Integer> itemCount = new HashMap<String, Integer>();
    static Logger log = Logger.getLogger(OrderItemVisit.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            String province_id = input.getStringByField(FName.PROVINCE_ID
                .name());
            String itemid = input.getString(0);
            Integer itemSum = itemCount(itemid);
            itemCount.put(itemid, itemSum);
            collector.emit(StreamId.ITEMTYPE.name(),
                new Values(itemid, itemSum));
        } catch (IllegalArgumentException e) {
            if (input.getSourceStreamId().equals(StreamId.SIGNAL24H.name())) {
                log.info("24Hour is coming.");
                itemCount.clear();
            }
        }
        collector.ack(input);
    }

    private int itemCount(String itemid) {
        int count = getItemCount(itemid);
        count++;
        return count;
    }

    private int getItemCount(String itemid) {
        Integer count = itemCount.get(itemid);
        if (count == null) {
            count = 0;
        }
        return count;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamId.ITEMTYPE.name(), new Fields(
            FName.ITEM_ID.name(), FName.COUNT.name()));
    }

    public void cleanup() {
    }
}

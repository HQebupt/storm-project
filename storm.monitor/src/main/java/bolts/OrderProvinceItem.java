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

public class OrderProvinceItem extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Map<String, Integer> provinceItem = new HashMap<String, Integer>();
    static Logger log = Logger.getLogger(OrderProvinceItem.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            String province_id = input.getStringByField(FName.PROVINCE_ID
                .name());
            String item_id = input.getString(0);
            String key = province_id + "|" + item_id;
            Integer itemSum = itemCount(key);
            provinceItem.put(key, itemSum);
            collector.emit(StreamId.PROVINCEITEM.name(), new Values(key,
                itemSum));
        } catch (IllegalArgumentException e) {
            if (input.getSourceStreamId().equals(StreamId.SIGNAL24H.name())) {
                log.info("24Hour is coming.");
                provinceItem.clear();
            }
        }
        collector.ack(input);
    }

    private int itemCount(String key) {
        Integer count = getItemCount(key);
        count++;
        return count;
    }

    private int getItemCount(String key) {
        Integer count = provinceItem.get(key);
        if (count == null) {
            count = 0;
        }
        return count;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamId.PROVINCEITEM.name(), new Fields(
            FName.PROVINCEITEM.name(), FName.COUNT.name()));
    }

    public void cleanup() {
    }
}

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

public class ProvinceItem extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Map<String, Integer> provinceItem = new HashMap<String, Integer>();
    static Logger log = Logger.getLogger(ProvinceItem.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String province_id = null;
        try {
            province_id = input.getStringByField(FName.PROVINCE_ID.name());
            Integer itemSum = itemCount(province_id);
            provinceItem.put(province_id, itemSum);
            collector.emit(new Values(province_id, itemSum));
        } catch (IllegalArgumentException e) {
            if (input.getSourceStreamId().equals(StreamId.SIGNAL24H.name())) {
                log.info("24Hour is coming.");
                provinceItem.clear();
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
        Integer count = provinceItem.get(province_id);
        if (count == null) {
            count = 0;
        }
        return count;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FName.PROVINCE_ID.name(), FName.COUNT
            .name()));
    }

    public void cleanup() {
    }
}

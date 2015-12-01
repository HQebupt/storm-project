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

public class OrdProPlatUV extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Map<String, Integer> proplatUv = new HashMap<String, Integer>();
    static Logger log = Logger.getLogger(OrdProPlatUV.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            String province = input.getStringByField(FName.PROVINCE_ID.name());
            String platform = input.getStringByField(FName.PLATFORM.name());
            String key = province + "|" + platform;
            Integer itemSum = itemCount(key);
            proplatUv.put(key, itemSum);
            collector.emit(new Values(key, itemSum));
        } catch (IllegalArgumentException e) {
            if (input.getSourceStreamId().equals(StreamId.SIGNAL24H.name())) {
                log.info("24Hour is coming.");
                proplatUv.clear();
            }
        }
        collector.ack(input);
    }

    private Integer itemCount(String platform) {
        int count = getItemCount(platform);
        count++;
        return count;
    }

    private int getItemCount(String platform) {
        Integer count = proplatUv.get(platform);
        if (count == null) {
            count = 0;
        }
        return count;
    }

    // 这儿用"PROVINCE_ID"这个名词是不准确的，实质上是"PROVINCE|PLATFORM"
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FName.PROVINCE_ID.name(), FName.COUNT
            .name()));
    }

    public void cleanup() {
    }
}

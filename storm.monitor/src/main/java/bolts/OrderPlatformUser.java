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

public class OrderPlatformUser extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Map<String, Integer> platformItem = new HashMap<String, Integer>();
    static Logger log = Logger.getLogger(OrderPlatformUser.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            String platform = input.getStringByField(FName.PLATFORM.name());
            Integer itemSum = itemCount(platform);
            platformItem.put(platform, itemSum);
            collector.emit(new Values(platform, itemSum));
        } catch (IllegalArgumentException e) {
            if (input.getSourceStreamId().equals(StreamId.SIGNAL24H.name())) {
                log.info("24Hour is coming.");
                platformItem.clear();
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
        Integer count = platformItem.get(platform);
        if (count == null) {
            count = 0;
        }
        return count;
    }

    /**
     * 如果设计合理，order.txt的platform字段位置可以考虑 和InterfaceLineSplitBolt和UserCleanBlot、
     * WapLineSplitBolt中的provice_id字段位置相同。 目前并不采用这样的方法,只采用
     * 复用"PROVINCE_ID"，而不是"PLATFORM"，是为了复用ProvinceItemBoltDB.java 这样做是为了应对需求变化。
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FName.PROVINCE_ID.name(), FName.COUNT.name()));
    }

    public void cleanup() {
    }
}

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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class OrderUserClean extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Set<String> userSet = new HashSet<String>();
    static Logger log = Logger.getLogger(OrderUserClean.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            String msisdn = input.getStringByField(FName.MSISDN.name());
            String provinceID = input
                .getStringByField(FName.PROVINCE_ID.name());
            String platform = input.getStringByField(FName.PLATFORM.name());
            String fee = input.getStringByField(FName.REALINFORFEE.name());
            Boolean isFee = fee.equalsIgnoreCase("0");
            Boolean isvisited = isVisted(msisdn);
            if ((!isvisited) && (!isFee)) {
                userSet.add(msisdn);
                collector.emit(new Values(platform, provinceID, msisdn));
            }
        } catch (IllegalArgumentException e) {
            if (input.getSourceStreamId().equals(StreamId.SIGNAL24H.name())) {
                log.info("24Hour is coming.");
                userSet.clear();
            }
        }
        collector.ack(input);
    }

    private Boolean isVisted(String msisdn) {
        return userSet.contains(msisdn);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FName.PLATFORM.name(), FName.PROVINCE_ID
            .name(), FName.MSISDN.name()));
    }

    public void cleanup() {
    }
}

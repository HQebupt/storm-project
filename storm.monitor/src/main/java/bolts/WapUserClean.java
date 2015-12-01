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

public class WapUserClean extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Set<String> userSet = new HashSet<String>();
    Set<String> ipSet = new HashSet<String>();
    static Logger log = Logger.getLogger(WapUserClean.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            String msisdn = input.getStringByField(FName.MSISDN.name());
            String provinceID = input
                .getStringByField(FName.PROVINCE_ID.name());
            String remoteip = input.getStringByField(FName.REMOTEIP.name());
            String firstNum = msisdn.length() >= 1 ? msisdn.substring(0, 1)
                : "0";
            Boolean isvisited;
            if (!firstNum.equalsIgnoreCase("9")) {
                isvisited = isVisted(msisdn);
                if (!isvisited) {
                    userSet.add(msisdn);
                    collector.emit(new Values(msisdn, provinceID));
                }
            } else {
                isvisited = isIPVisted(remoteip);
                if (!isvisited) {
                    ipSet.add(remoteip);
                    collector.emit(new Values(msisdn, provinceID));
                }
            }
        } catch (IllegalArgumentException e) {
            if (input.getSourceStreamId().equals(StreamId.SIGNAL24H.name())) {
                log.info("24Hour is coming.");
                userSet.clear();
                ipSet.clear();
            }
        }
        collector.ack(input);
    }

    private Boolean isIPVisted(String remoteip) {
        return ipSet.contains(remoteip);
    }

    private Boolean isVisted(String msisdn) {
        return userSet.contains(msisdn);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FName.MSISDN.name(), FName.PROVINCE_ID
            .name()));
    }

    public void cleanup() {
    }
}

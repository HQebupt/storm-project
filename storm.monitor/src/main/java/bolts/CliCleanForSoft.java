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

public class CliCleanForSoft extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Set<String> userSet = new HashSet<String>();
    Set<String> ipSet = new HashSet<String>();
    static Logger log = Logger.getLogger(CliCleanForSoft.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            String msisdn = input.getStringByField(FName.MSISDN.name());
            String wapip = input.getStringByField(FName.WAPIP.name());
            String type = input.getStringByField(FName.ACCESSTYPE.name())
                .trim();
            String uaname = input.getStringByField(FName.UANAME.name());
            Boolean accType = isSoft(type);
            Boolean isMM = ismm(uaname);
            if (accType && (!isMM)) {
                String firstNum = msisdn.substring(0, 1);
                Boolean isvisited;
                if (!firstNum.equalsIgnoreCase("9")) {
                    isvisited = isVisted(msisdn);
                    if (!isvisited) {
                        userSet.add(msisdn);
                        collector.emit(new Values(msisdn));
                    }
                } else {
                    isvisited = isIPVisted(wapip);
                    if (!isvisited) {
                        ipSet.add(wapip);
                        collector.emit(new Values(wapip));
                    }
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

    private Boolean isSoft(String type) {
        return type.equalsIgnoreCase("16") || type.equalsIgnoreCase("4");
    }

    private Boolean ismm(String s) {
        Boolean mm = false;
        if (s.length() >= 2) {
            s = s.substring(0, 2);
            mm = s.equalsIgnoreCase("MM");
        }
        return mm;
    }

    private Boolean isIPVisted(String remoteip) {
        return ipSet.contains(remoteip);
    }

    private Boolean isVisted(String msisdn) {
        return userSet.contains(msisdn);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FName.MSISDN.name()));
    }

    public void cleanup() {
    }
}

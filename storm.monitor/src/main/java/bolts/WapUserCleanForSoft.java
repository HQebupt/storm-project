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

public class WapUserCleanForSoft extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Set<String> userSet = new HashSet<String>();
    Set<String> ipSet = new HashSet<String>();
    static Logger log = Logger.getLogger(WapUserCleanForSoft.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            String msisdn = input.getStringByField(FName.MSISDN.name());
            String remoteip = input.getStringByField(FName.REMOTEIP.name());
            String terminal = input.getStringByField(FName.TERMINAL.name());
            Boolean accType = input.getStringByField(FName.ACCESSTYPE.name())
                .trim().equalsIgnoreCase("16");
            Boolean isMM = ismm(terminal);
            if (accType && (!isMM) && (msisdn.length() >= 1)) {
                String firstNum = msisdn.substring(0, 1);
                Boolean isvisited;
                if (!firstNum.equalsIgnoreCase("9")) {
                    isvisited = isVisted(msisdn);
                    if (!isvisited) {
                        userSet.add(msisdn);
                        collector.emit(new Values(msisdn));
                    }
                } else {
                    isvisited = isIPVisted(remoteip);
                    if (!isvisited) {
                        ipSet.add(remoteip);
                        collector.emit(new Values(remoteip));
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

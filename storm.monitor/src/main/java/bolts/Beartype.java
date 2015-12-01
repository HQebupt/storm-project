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

import java.util.Map;

public class Beartype extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    static Logger log = Logger.getLogger(Beartype.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String btype = input.getStringByField(FName.BEARTYPE.name());
        String rtime = input.getStringByField(FName.RECORDTIME.name());
        btype = build(btype.trim());
        collector.emit(new Values(btype, rtime));
        collector.ack(input);
    }

    private String build(String btype) {
        if (btype.equalsIgnoreCase("2"))
            return "2";
        else
            return "3";
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FName.BEARTYPE.name(), FName.RECORDTIME.name()));
    }

    public void cleanup() {
    }
}

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

import java.util.Map;

public class Total extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    static Integer total = 0;
    static Logger log = Logger.getLogger(Total.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            String clear = input.getStringByField(FName.MSISDN.name());
            total++;
            collector.emit(new Values(total));
        } catch (IllegalArgumentException e) {
            if (input.getSourceStreamId().equals(StreamId.SIGNAL24H.name())) {
                log.info("24Hour is coming.");
                total = 0;
            }
        }
        collector.ack(input);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FName.TOTAL.name()));
    }

    public void cleanup() {
    }
}

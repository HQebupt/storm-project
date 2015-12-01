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

public class CellFeeSum extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    static Integer feeTotal = 0;
    static Logger log = Logger.getLogger(CellFeeSum.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            String fee = input.getStringByField(FName.FEE.name());
            feeTotal += Integer.valueOf(fee);
            collector.emit(new Values(feeTotal));
        } catch (IllegalArgumentException e) {
            if (input.getSourceStreamId().equals(StreamId.SIGNAL24H.name())) {
                log.info("24Hour is coming.");
                feeTotal = 0;
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

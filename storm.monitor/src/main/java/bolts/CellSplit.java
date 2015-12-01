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

public class CellSplit extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    static Logger log = Logger.getLogger(CellSplit.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String line = input.getString(0);
        String[] words = line.split("\\|", -1);
        if (words.length >= 3) {
            String msisdn = words[0];
            String ordertime = words[1];
            String fee = words[2];
            collector.emit(input, new Values(msisdn.trim(), ordertime.trim(),
                fee.trim()));
            collector.ack(input);
        } else {
            log.info("Error data: " + line);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FName.MSISDN.name(), FName.ORDERTIME.name(), FName.FEE.name()));
    }
}

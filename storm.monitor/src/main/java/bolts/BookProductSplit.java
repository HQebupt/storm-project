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

public class BookProductSplit extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    static Logger log = Logger.getLogger(BookProductSplit.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String line = input.getString(0);
        String[] words = line.split("\\|", -1);
        if (words.length >= 3) {
            String recordTime = words[0];
            String itemID = words[1];
            String itemType = words[2];
            collector.emit(StreamId.ITEMINFO.name(), new Values(itemID, itemType,
                recordTime));
            collector.ack(input);
        } else {
            log.info("Error data: " + line);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamId.ITEMINFO.name(), new Fields(FName.ITEM_ID.name(),
            FName.ITEMTYPE.name(), FName.RECORDTIME.name()));
    }
}

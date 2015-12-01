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

public class BookSplit extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    static Logger log = Logger.getLogger(BookSplit.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String line = input.getString(0);
        String[] words = line.split("\\|", -1);
        if (words.length >= 4) {
            String book_id = words[1];
            String type_id = words[2];
            String class_id = words[3];
            String com = buildkey(type_id, class_id);
            collector.emit(StreamId.ITEMINFO.name(), new Values(book_id.trim(), com));
            collector.ack(input);
        } else {
            log.info("Error data: " + line);
        }
    }

    private String buildkey(String type_id, String class_id) {
        return type_id.trim() + "|" + class_id.trim();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamId.ITEMINFO.name(), new Fields(FName.ITEM_ID.name(),
            FName.COM.name()));
    }
}

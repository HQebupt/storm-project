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

public class ChapterSplit extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    static Logger log = Logger.getLogger(ChapterSplit.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String line = input.getString(0);
        String[] words = line.split("\\|", -1);
        if (words.length >= 5) {
            String msisdn = words[0];
            String book_id = words[2];
            String chapter_id = words[3];
            String province_id = words[4];
            collector.emit(new Values(msisdn.trim(), book_id.trim(), chapter_id
                .trim(), province_id.trim()));
            collector.ack(input);
        } else {
            log.info("Error data: " + line);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FName.MSISDN.name(), FName.BOOK_ID.name(),
            FName.CHAPTER_ID.name(), FName.PROVINCE_ID.name()));
    }
}

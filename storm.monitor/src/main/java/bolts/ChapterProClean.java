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

public class ChapterProClean extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Set<String> visitSet = new HashSet<String>();
    static Logger log = Logger.getLogger(ChapterProClean.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            String msisdn = input.getStringByField(FName.MSISDN.name());
            String book_id = input.getStringByField(FName.BOOK_ID.name());
            String chapter_id = input.getStringByField(FName.CHAPTER_ID.name());
            String province_id = input.getStringByField(FName.PROVINCE_ID.name());
            String key = buildkey(msisdn, book_id, chapter_id, province_id);
            Boolean isvisited = isVisted(key);
            if (!isvisited) {
                visitSet.add(key);
                collector.emit(new Values(book_id, province_id));
            }
        } catch (IllegalArgumentException e) {
            if (input.getSourceStreamId().equals(StreamId.SIGNAL24H.name())) {
                log.info("24Hour is coming.");
                visitSet.clear();
            }
        }
        collector.ack(input);
    }

    private String buildkey(String msisdn, String book_id, String chapter_id,
                            String province_id) {
        return msisdn + "|" + book_id + "|" + chapter_id + "|" + province_id;
    }

    private Boolean isVisted(String msisdn) {
        return visitSet.contains(msisdn);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FName.BOOK_ID.name(), FName.PROVINCE_ID.name()));
    }

    public void cleanup() {
    }
}

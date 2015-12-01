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

public class OrderSplit extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    static Logger log = Logger.getLogger(OrderSplit.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String line = input.getString(0);
        String[] words = line.split("\\|", -1);
        if (words.length >= 9) {
            String msisdn = words[0];
            String recordTime = words[1];
            String platform = words[2];
            String ordertype = words[3];
            String product_id = words[4];
            String book_id = words[5];
            String province_id = words[6];
            String realinforfee = words[7];
            String ticketid = words[8];
            collector.emit(StreamId.ORDER.name(), new Values(msisdn,
                recordTime, platform, ordertype, product_id, book_id,
                province_id, realinforfee, ticketid));
            collector.ack(input);
        } else {
            log.info("Error data: " + line);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamId.ORDER.name(),
            new Fields(FName.MSISDN.name(), FName.RECORDTIME.name(),
                FName.PLATFORM.name(), FName.ORDERTYPE.name(),
                FName.PRODUCT_ID.name(), FName.BOOK_ID.name(),
                FName.PROVINCE_ID.name(), FName.REALINFORFEE.name(),
                FName.TICKETID.name()));
    }
}

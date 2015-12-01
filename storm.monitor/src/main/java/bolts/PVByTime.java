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

public class PVByTime extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    static Integer PVTotal = new Integer(0);
    int min = 0;
    int max = 0;
    static Logger log = Logger.getLogger(PVByTime.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String data = null;
        try {
            data = input.getStringByField(FName.MSISDN.name());
        } catch (IllegalArgumentException e) {
        }
        if (data != null) {
            String recordTime = input.getStringByField(FName.RECORDTIME.name());
            PVTotal++;
            maxmin(recordTime);
        } else {
            if (input.getSourceStreamId().equals(StreamId.SIGNAL1MIN.name())) {
                data = input.getStringByField(FName.ACTION1MIN.name());
                if (PVTotal != 0) {
                    Integer sub = max - min + 1;
                    Double pv = (double) PVTotal;
                    collector.emit(new Values(pv, sub));
                    log.info("1min PVByTime: " + pv + "max:" + max + "min:"
                        + min);
                }
                clear();
            }
        }
        collector.ack(input);
    }

    private void clear() {
        PVTotal = 0;
        min = 0;
        max = 0;
    }

    private void maxmin(String recordTime) {
        String secStr = recordTime.substring(12, 14);
        int sec = Integer.parseInt(secStr);
        if (sec > max) {
            max = sec;
        } else if (sec < min) {
            min = sec;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FName.PVPERMIN.name(), FName.SUB.name()));
    }

    public void cleanup() {
    }
}

package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import db.DB;
import org.apache.log4j.Logger;
import util.FName;
import util.StreamId;

import java.text.DecimalFormat;
import java.util.Map;

public class BtPVRateDB extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    static Double pvSum = new Double(0.0);
    static int sum = 0;
    String btype = "";
    private String tableName;
    private DecimalFormat dformat = new DecimalFormat("0.00000");
    private DB db = new DB();
    static Logger log = Logger.getLogger(BtPVRateDB.class);

    public BtPVRateDB(String tableName) {
        this.tableName = tableName;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        Double data = null;
        try {
            data = input.getDoubleByField(FName.PVPERMIN.name());
        } catch (IllegalArgumentException e) {
        }
        if (data != null) {
            btype = input.getStringByField(FName.BEARTYPE.name());
            Integer sub = input.getIntegerByField(FName.SUB.name());
            pvSum += data;
            sum += sub;
        } else {
            if (input.getSourceStreamId().equals(StreamId.SIGNAL15MIN.name())) {
                String action = input.getStringByField(FName.ACTION15MIN.name());
                String timePeriod = action.trim();
                if (sum != 0) {
                    Double pv = pvSum / sum;
                    String pvBytime = dformat.format(pv);
                    log.info("15min BearType: " + btype + "  PV by time:"
                        + pvBytime + "  pvSum:" + pvSum + "  count:" + sum);
                    downloadToDB(timePeriod, btype, pvBytime);
                }
                clear();
            }
        }
        collector.ack(input);
    }

    private void clear() {
        pvSum = 0.0;
        sum = 0;
        btype = "";
    }

    private void downloadToDB(String timePeriod, String btype, String pvBytime) {
        db.insertUser(tableName, timePeriod, btype, pvBytime);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public void cleanup() {
    }
}

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

public class DelayInterfaceSplit extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    static Logger log = Logger.getLogger(DelayInterfaceSplit.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String line = input.getString(0);
        String[] words = line.split("\\|", -1);
        if (words.length >= 8) {
            String msisdn = words[0];
            String action = words[1];
            String startTime = words[2];
            String time = words[3];
            String beartype = words[4];
            String portalIp = words[5];
            String resultCode = words[6];
            String sceneType = words[7];
            String fchar = action.trim().substring(0, 1);
            int error = action.indexOf('?');
            if (!fchar.equalsIgnoreCase("/") && (error == -1)) {
                collector.emit(input, new Values(action.trim(), time,
                    startTime, beartype, msisdn, portalIp, resultCode,
                    sceneType));
            }
            collector.ack(input);
        } else {
            log.info("Error data: " + line);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FName.ACTION.name(), FName.TIME.name(),
            FName.STARTTIME.name(), FName.BEARTYPE.name(), FName.MSISDN
            .name(), FName.PORTALIP.name(),
            FName.RESULTCODE.name(), FName.SCENETYPE.name()));
    }
}

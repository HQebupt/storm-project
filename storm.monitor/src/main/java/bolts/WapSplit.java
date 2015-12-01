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

/**
 * @author HuangQiang
 * @version 1.0 2014-1-9-11-24:update execute() and declareOutputFields()
 *          according to uespagevisit.txt, which has 8
 *          fields.(//RECORDTIME|RECORDTIMELEN
 *          |BEARTYPE|ACCESSTYPE|PAGENAME|MSISDN|PROVINCEID|CITYID|ERRCODE)
 * @date 2014-1-9上午11:22:18
 * @file WapSplit.java
 */
public class WapSplit extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    static Logger log = Logger.getLogger(WapSplit.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String line = input.getString(0);
        String[] words = line.split("\\|", -1);
        if (words.length >= 14) {
            String msisdn = words[5];
            String recordTime = words[0];
            String accesstype = words[3];
            String province_id = words[6];
            String city_id = words[7];
            String pagename = words[4];
            String recordTimeLen = words[1];
            String beartype = words[2];
            String errcode = words[8];
            String remoteip = words[9];
            String terminal = words[13];
            collector.emit(input, new Values(pagename, recordTimeLen,
                accesstype, province_id, city_id, terminal, msisdn,
                beartype, errcode, remoteip, recordTime));
            collector.ack(input);
        } else {
            log.info("Error data: " + line);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FName.PAGENAME.name(), FName.RECORDTIMELEN
            .name(), FName.ACCESSTYPE.name(), FName.PROVINCE_ID.name(),
            FName.CITY_ID.name(), FName.TERMINAL.name(), FName.MSISDN
            .name(), FName.BEARTYPE.name(), FName.ERRCODE.name(),
            FName.REMOTEIP.name(), FName.RECORDTIME.name()));
        // 将"BEARTYPE"位置提前了，使得BEARTYPE对应1，PAGENAME对应0
    }
}

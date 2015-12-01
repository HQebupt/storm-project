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

public class InterfaceSplit extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    static Logger log = Logger.getLogger(InterfaceSplit.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String line = input.getString(0);
        String[] words = line.split("\\|", -1);
        if (words.length >= 11) {
            String msisdn = words[0];
            String recordTime = words[1];
            String accesstype = words[2];
            String province_id = words[3];
            String city_id = words[4];
            String action = words[5].trim();// 过滤掉字符串结尾有\n的action
            String beartype = words[6];
            String wapip = words[8];
            String uaName = words[10];
            collector.emit(StreamId.INTERFACEDATA.name(), new Values(action,
                beartype, accesstype, province_id, city_id, wapip, msisdn,
                uaName, recordTime));
            collector.ack(input);
        } else {
            log.info("Error data: " + line);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(
            StreamId.INTERFACEDATA.name(),
            new Fields(FName.ACTION.name(), FName.BEARTYPE.name(),
                FName.ACCESSTYPE.name(), FName.PROVINCE_ID.name(),
                FName.CITY_ID.name(), FName.WAPIP.name(), FName.MSISDN
                .name(), FName.UANAME.name(), FName.RECORDTIME.name()));
        // 提前了BEARTYPE到ACTION的前面，使得BEARTYPE对应1，ACTION对应0.为了复用求PV占比的函数（PVRateBolt），
        // 如果以后这个地方字段顺序需要改变，需要修改PVRateBolt。
    }
}
